package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.DomainIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef, Schemas}
import ch.epfl.bluebrain.nexus.kg.core.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import SchemaRoutesSpec._
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.routes.SparqlFixtures._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class SchemaRoutesSpec
  extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with Resources
    with ScalaFutures {

  private implicit val mt = ActorMaterializer()(system)
  private implicit val ec = system.dispatcher

  override implicit val patienceConfig = PatienceConfig(3 seconds, 100 millis)

  "A SchemaRoutes" should {
    val sparqlUri = Uri("http://localhost:9999/bigdata/sparql")
    val vocab = baseUri.copy(path = baseUri.path / "core")

    val schemaJson = jsonContentOf("/int-value-schema.json")
    val shapeNodeShape = jsonContentOf("/int-value-shape-nodeshape.json")
    val orgAgg = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs = Organizations(orgAgg)
    val domAgg = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms = Domains(domAgg, orgs)
    val schAgg = MemoryAggregate("schemas")(Schemas.initial, Schemas.next, Schemas.eval).toF[Future]
    val schemas = Schemas(schAgg, doms, baseUri.toString)

    val orgRef = Await.result(orgs.create(OrgId(genString(length = 3)), Json.obj()), 2 seconds)
    val domRef = Await.result(doms.create(DomainId(orgRef.id, genString(length = 5)), genString(length = 8)), 2 seconds)

    implicit val client: UntypedHttpClient[Future] = fixedHttpClient(fixedResponse("/list_schemas_sparql_result.json"))
    val sparqlClient = SparqlClient[Future](sparqlUri)

    val querySettings = QuerySettings(Pagination(0L, 20), "some-index", vocab)

    val route = SchemaRoutes(schemas, sparqlClient, querySettings, baseUri).routes

    val schemaId = SchemaId(domRef.id, genString(length = 8), genVersion())

    "create a schema" in {
      Put(s"/schemas/${schemaId.show}", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 1L))
      }
    }

    "reject the creation of a schema that already exists" in {
      Put(s"/schemas/${schemaId.show}", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[SchemaAlreadyExists.type]
      }
    }

    "reject the creation of a schema with illegal version format" in {
      val id = schemaId.show.replace(schemaId.version.show, "v1.0")
      Put(s"/schemas/$id", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }

    "reject the creation of a schema with illegal name format" in {
      Put(s"/schemas/${schemaId.copy(name = "@!").show}", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidSchemaId.type]
      }
    }

    "return the current schema" in {
      Get(s"/schemas/${schemaId.show}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "@id" -> Json.fromString(s"$baseUri/schemas/${schemaId.show}"),
          "rev" -> Json.fromLong(1L),
          "deprecated" -> Json.fromBoolean(false),
          "published" -> Json.fromBoolean(false)
        ).deepMerge(schemaJson)
      }
    }

    "return list of schemas from organization" in {
      Get(s"/schemas/org") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Results] shouldEqual fixedListSchemas(Uri("http://example.com/schemas/org"))
      }
    }

    "return list of schemas from domain id with specific pagination" in {
      val specificPagination = Pagination(0L, 10)
      Get(s"/schemas/org/domain?from=${specificPagination.from}&size=${specificPagination.size}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Results] shouldEqual fixedListSchemas(Uri(s"http://example.com/schemas/org/domain?from=${specificPagination.from}&size=${specificPagination.size}"))
      }
    }

    "return a list of schemas from schema name with deprecated results" in {
      Get(s"/schemas/org/domain/subject?deprecated=true") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Results] shouldEqual fixedListSchemas(Uri(s"http://example.com/schemas/org/domain/subject?deprecated=true"))
      }
    }

    "update a schema" in {
      Put(s"/schemas/${schemaId.show}?rev=1", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 2L))
      }
    }

    "reject updating a schema which does not exist" in {
      Put(s"/schemas/${schemaId.copy(name = "another").show}?rev=1", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[SchemaDoesNotExist.type]
      }
    }

    "reject updating a schema with incorrect rev" in {
      Put(s"/schemas/${schemaId.show}?rev=10", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "publish a schema" in {
      Patch(s"/schemas/${schemaId.show}/config?rev=2", SchemaConfig(published = true)) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 3L))
      }
      schemas.fetch(schemaId).futureValue shouldEqual Some(Schema(schemaId, 3L, schemaJson, deprecated = false, published = true))
    }

    "return specific schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeShape2") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Some[Json]] shouldEqual
          Some(shapeNodeShape.deepMerge(
            Json.obj(
              "@id" -> Json.fromString(s"$baseUri/schemas/${schemaId.show}/shapes/IdNodeShape2"),
              "rev" -> Json.fromLong(3L),
              "deprecated" -> Json.fromBoolean(false),
              "published" -> Json.fromBoolean(true)
            )
          ))
      }
    }

    "reject fetching non existing schema shape" in {
      Get(s"/schemas/${schemaId.show}/shapes/IdNodeS") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "reject publishing a schema when setting passing a config with published=false" in {
      Patch(s"/schemas/${schemaId.show}/config?rev=2", SchemaConfig(published = false)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUnpublishSchema.type]
      }
    }

    "reject updating a schema when it is published" in {
      Put(s"/schemas/${schemaId.show}?rev=3", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUpdatePublished.type]
      }
    }

    "deprecate a schema" in {
      Delete(s"/schemas/${schemaId.show}?rev=3") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual schemaRefAsJson(SchemaRef(schemaId, 4L))
      }
      schemas.fetch(schemaId).futureValue shouldEqual Some(Schema(schemaId, 4L, schemaJson, deprecated = true, published = true))
    }

    "reject updating a schema when it is deprecated" in {
      Put(s"/schemas/${schemaId.show}?rev=4", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[SchemaIsDeprecated.type]
      }
    }

    "reject the creation of schema from a deprecated domain" in {
      //Deprecate the domain
      doms.deprecate(domRef.id, domRef.rev).futureValue
      //Create a SchemaId from the deprecated domain
      val schemaId2 = SchemaId(domRef.id, genString(length = 8), genVersion())

      Put(s"/schemas/${schemaId2.show}", schemaJson) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[DomainIsDeprecated.type]
      }
    }
  }
}

object SchemaRoutesSpec {
  private val baseUri = Uri("http://localhost/v0")

  import cats.syntax.show._

  private def schemaRefAsJson(ref: SchemaRef) = Json.obj(
    "@id" -> Json.fromString(s"$baseUri/schemas/${ref.id.show}"),
    "rev" -> Json.fromLong(ref.rev))

  final case class Result(resultId: String, source: Source)

  final case class Results(total: Long, results: List[Result], links: List[Link])

  private def fixedListSchemas(uri: Uri) =
    Results(2L, List(
      Result(
        s"$baseUri/schemas/org/domain/subject/v1.0.0",
        Source(
          s"$baseUri/schemas/org/domain/subject/v1.0.0",
          List(Link("self", s"$baseUri/schemas/org/domain/subject/v1.0.0")))),
      Result(
        s"$baseUri/schemas/org/domain/subject2/v1.0.0",
        Source(
          s"$baseUri/schemas/org/domain/subject2/v1.0.0",
          List(Link("self", s"$baseUri/schemas/org/domain/subject2/v1.0.0")))),
    ), List(Link("self", uri)))
}
