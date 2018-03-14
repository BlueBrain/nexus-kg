package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.kamon.directives.TracingDirectives
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.DomainIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.linksEncoder
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutesSpec._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class SchemaRoutesSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with Resources
    with ScalaFutures
    with MockedIAMClient {

  private implicit val mt: ActorMaterializer        = ActorMaterializer()(system)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 100 millis)

  "A SchemaRoutes" should {
    val sparqlUri = Uri("http://localhost:9999/bigdata/sparql")
    val vocab     = baseUri.copy(path = baseUri.path / "core")

    val schemaJson       = jsonContentOf("/int-value-schema.json")
    val schemaJsonObject = schemaJson.asObject.get
    val jsonContext =
      Json.arr(schemaJsonObject("@context").getOrElse(Json.obj()), Json.fromString(prefixes.CoreContext.toString))
    val schemaJsonWithStandardsContext = Json.fromJsonObject(
      schemaJsonObject.add("@context", jsonContext)
    )

    val shapeNodeShape                      = jsonContentOf("/int-value-shape-nodeshape.json")
    val orgAgg                              = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs                                = Organizations(orgAgg)
    val domAgg                              = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms                                = Domains(domAgg, orgs)
    val ctxAgg                              = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
    implicit val contexts                   = Contexts(ctxAgg, doms, baseUri.toString())
    val schAgg                              = MemoryAggregate("schemas")(Schemas.initial, Schemas.next, Schemas.eval).toF[Future]
    val schemas                             = Schemas(schAgg, doms, contexts, baseUri.toString)
    implicit val clock                      = Clock.systemUTC
    implicit val tracing: TracingDirectives = TracingDirectives()

    val caller = CallerCtx(clock, AnonymousCaller(Anonymous()))

    val orgRef = Await.result(orgs.create(OrgId(genString(length = 3)), Json.obj())(caller), 2 seconds)
    val domRef =
      Await.result(doms.create(DomainId(orgRef.id, genString(length = 5)), genString(length = 8))(caller), 2 seconds)

    val sparqlClient = SparqlClient[Future](sparqlUri, None)

    val querySettings                  = QuerySettings(Pagination(0L, 20), 100, vocab, baseUri)
    implicit val cl: IamClient[Future] = iamClient("http://localhost:8080")

    val indexingSettings   = ElasticIndexingSettings("", "", sparqlUri, sparqlUri)
    val elasticQueryClient = ElasticQueryClient[Future](sparqlUri)

    val elasticClient = ElasticClient[Future](sparqlUri, elasticQueryClient)

    val route =
      SchemaRoutes(schemas, sparqlClient, elasticClient, indexingSettings, querySettings, baseUri).routes

    val schemaId = SchemaId(domRef.id, genString(length = 8), genVersion())

    createNexusContexts(orgs, doms, contexts)(caller)

    "create a schema" in {
      Put(s"/schemas/${schemaId.show}", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 1L))
      }
    }

    "reject the creation of a schema that already exists" in {
      Put(s"/schemas/${schemaId.show}", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[SchemaAlreadyExists.type]
      }
    }

    "reject the creation of a schema with illegal version format" in {
      val id = schemaId.show.replace(schemaId.version.show, "v1.0")
      Put(s"/schemas/$id", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }

    "reject the creation of a schema with illegal name format" in {
      Put(s"/schemas/${schemaId.copy(name = "@!").show}", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidSchemaId.type]
      }
    }

    "return the current schema" in {
      Get(s"/schemas/${schemaId.show}") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        responseAs[Json] shouldEqual Json
          .obj(
            "@id"     -> Json.fromString(s"$baseUri/schemas/${schemaId.show}"),
            "nxv:rev" -> Json.fromLong(1L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/schemas/${schemaId.show}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false),
            "nxv:published"  -> Json.fromBoolean(false)
          )
          .deepMerge(schemaJsonWithStandardsContext)
      }
    }

    "return the current schema with a custom format" in {
      Get(s"/schemas/${schemaId.show}?format=expanded") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
      }
    }

    "update a schema" in {
      Put(s"/schemas/${schemaId.show}?rev=1", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 2L))
      }
    }

    "reject updating a schema which does not exist" in {
      Put(s"/schemas/${schemaId.copy(name = "another").show}?rev=1", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[SchemaDoesNotExist.type]
      }
    }

    "reject updating a schema with incorrect rev" in {
      Put(s"/schemas/${schemaId.show}?rev=10", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "publish a schema" in {
      Patch(s"/schemas/${schemaId.show}/config?rev=2", SchemaConfig(published = true)) ~> addCredentials(
        ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 3L))
      }
      schemas.fetch(schemaId).futureValue shouldEqual Some(
        Schema(schemaId, 3L, schemaJson, deprecated = false, published = true))
    }

    "fetch old revision of a schema" in {
      Get(s"/schemas/${schemaId.show}?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json
          .obj(
            "@id"     -> Json.fromString(s"$baseUri/schemas/${schemaId.show}"),
            "nxv:rev" -> Json.fromLong(1L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/schemas/${schemaId.show}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false),
            "nxv:published"  -> Json.fromBoolean(false)
          )
          .deepMerge(schemaJsonWithStandardsContext)
      }
    }

    "return not found for unknown revision of a schema" in {
      Get(s"/schemas/${schemaId.show}?rev=4") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return specific schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeShape2") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Some[Json]] shouldEqual
          Some(
            shapeNodeShape.deepMerge(
              Json.obj(
                "@context"       -> jsonContext,
                "@id"            -> Json.fromString(s"$baseUri/schemas/${schemaId.show}/shapes/IdNodeShape2"),
                "nxv:rev"        -> Json.fromLong(3L),
                "nxv:deprecated" -> Json.fromBoolean(false),
                "nxv:published"  -> Json.fromBoolean(true)
              )
            ))
      }
    }

    "return old revision from a schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeShape2?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Some[Json]] shouldEqual
          Some(
            shapeNodeShape.deepMerge(
              Json.obj(
                "@context"       -> jsonContext,
                "@id"            -> Json.fromString(s"$baseUri/schemas/${schemaId.show}/shapes/IdNodeShape2"),
                "nxv:rev"        -> Json.fromLong(1L),
                "nxv:deprecated" -> Json.fromBoolean(false),
                "nxv:published"  -> Json.fromBoolean(false)
              )
            ))
      }
    }

    "reject fetching non existing schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeS") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "reject fetching non existing revision of a schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeShape2?rev=5") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "reject publishing a schema when setting passing a config with published=false" in {
      Patch(s"/schemas/${schemaId.show}/config?rev=2", SchemaConfig(published = false)) ~> addCredentials(
        ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUnpublishSchema.type]
      }
    }

    "reject updating a schema when it is published" in {
      Put(s"/schemas/${schemaId.show}?rev=3", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUpdatePublished.type]
      }
    }

    "deprecate a schema" in {
      Delete(s"/schemas/${schemaId.show}?rev=3") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 4L))
      }
      schemas.fetch(schemaId).futureValue shouldEqual Some(
        Schema(schemaId, 4L, schemaJson, deprecated = true, published = true))
    }

    "reject updating a schema when it is deprecated" in {
      Put(s"/schemas/${schemaId.show}?rev=4", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[SchemaIsDeprecated.type]
      }
    }

    "reject the creation of schema from a deprecated domain" in {
      //Deprecate the domain
      doms.deprecate(domRef.id, domRef.rev)(caller).futureValue
      //Create a SchemaId from the deprecated domain
      val schemaId2 = SchemaId(domRef.id, genString(length = 8), genVersion())

      Put(s"/schemas/${schemaId2.show}", schemaJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[DomainIsDeprecated.type]
      }
    }
  }
}

object SchemaRoutesSpec {
  import cats.syntax.show._

  private def schemaRefAsJson(ref: SchemaRef) =
    Json.obj(
      "@context" -> Json.fromString(prefixes.CoreContext.toString),
      "@id"      -> Json.fromString(s"$baseUri/schemas/${ref.id.show}"),
      "nxv:rev"  -> Json.fromLong(ref.rev)
    )
}
