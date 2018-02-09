package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.{ActorMaterializer, Materializer}
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.DomainIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.linksEncoder
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes.ContextConfig
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutesSpec._
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class ContextRoutesSpec
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

  "ContextRoutes" should {

    val contextJson       = jsonContentOf("/contexts/shacl.json")
    val contextJsonObject = contextJson.asObject.get

    val contextJsonWithStandards = Json.obj(
      "@context" -> Json.arr(contextJsonObject("@context").getOrElse(Json.obj()),
                             Json.fromString(prefixes.CoreContext.toString)))

    val orgAgg            = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs              = Organizations(orgAgg)
    val domAgg            = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms              = Domains(domAgg, orgs)
    val ctxAgg            = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
    implicit val contexts = Contexts(ctxAgg, doms, baseUri.toString())
    implicit val clock    = Clock.systemUTC

    val caller = CallerCtx(clock, AnonymousCaller(Anonymous()))
    val orgRef = Await.result(orgs.create(OrgId(genString(length = 3)), Json.obj())(caller), 2 seconds)

    val domRef =
      Await.result(doms.create(DomainId(orgRef.id, genString(length = 5)), genString(length = 8))(caller), 2 seconds)

    val sparql                         = sparqlClient()
    implicit val cl: IamClient[Future] = iamClient("http://localhost:8080")

    val vocab              = baseUri.copy(path = baseUri.path / "core")
    val querySettings      = QuerySettings(Pagination(0L, 20), 100, "some-index", vocab, baseUri)
    val sparqlUri          = Uri("http://localhost:9999/bigdata/sparql")
    val indexingSettings   = ElasticIndexingSettings("", "", sparqlUri, sparqlUri)
    val elasticQueryClient = ElasticQueryClient[Future](sparqlUri)

    val elasticClient = ElasticClient[Future](sparqlUri, elasticQueryClient)

    val route = ContextRoutes(sparql, elasticClient, indexingSettings, querySettings, baseUri).routes

    val contextId = ContextId(domRef.id, genString(length = 8), genVersion())

    createNexusContexts(orgs, doms, contexts)(caller)

    "create a context" in {
      Put(s"/contexts/${contextId.show}", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual contextRefAsJson(ContextRef(contextId, 1L))
      }
    }

    "reject the creation of a context that already exists" in {
      Put(s"/contexts/${contextId.show}", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[ContextAlreadyExists.type]
      }
    }

    "reject the creation of a context with illegal version format" in {
      val id = contextId.show.replace(contextId.version.show, "v1.0")
      Put(s"/contexts/$id", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }

    "reject the creation of a context with illegal name format" in {
      Put(s"/contexts/${contextId.copy(name = "@!").show}", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidContextId.type]
      }
    }

    "return the current context" in {
      Get(s"/contexts/${contextId.show}") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        responseAs[Json] shouldEqual Json
          .obj(
            "@id"     -> Json.fromString(s"$baseUri/contexts/${contextId.show}"),
            "nxv:rev" -> Json.fromLong(1L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/contexts/${contextId.show}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false),
            "nxv:published"  -> Json.fromBoolean(false)
          )
          .deepMerge(contextJsonWithStandards)
      }
    }

    "return the current context with a custom format" in {
      Get(s"/contexts/${contextId.show}?format=expanded") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
      }
    }

    "update a context" in {
      Put(s"/contexts/${contextId.show}?rev=1", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual contextRefAsJson(ContextRef(contextId, 2L))
      }
    }

    "reject updating a context which doesn't exits" in {
      Put(s"/contexts/${contextId.copy(name = "another").show}?rev=1", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[ContextDoesNotExist.type]
      }
    }

    "reject updating a context with incorrect rev" in {
      Put(s"/contexts/${contextId.show}?rev=10", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "publish a context" in {
      Patch(s"/contexts/${contextId.show}/config?rev=2", ContextConfig(published = true)) ~> addCredentials(
        ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual contextRefAsJson(ContextRef(contextId, 3L))
      }
      contexts.fetch(contextId).futureValue shouldEqual Some(
        Context(contextId, 3L, contextJson, deprecated = false, published = true))
    }

    "reject publishing a context when setting passing a config with published=false" in {
      Patch(s"/contexts/${contextId.show}/config?rev=3", ContextConfig(published = false)) ~> addCredentials(
        ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUnpublishContext.type]
      }
    }

    "fetch old revision of a context" in {
      Get(s"/contexts/${contextId.show}?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json
          .obj(
            "@id"     -> Json.fromString(s"$baseUri/contexts/${contextId.show}"),
            "nxv:rev" -> Json.fromLong(1L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/contexts/${contextId.show}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false),
            "nxv:published"  -> Json.fromBoolean(false)
          )
          .deepMerge(contextJsonWithStandards)
      }
    }

    "return not found for attempting to fetch an unexisting context" in {
      Get(s"/contexts/${contextId.show}123") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return not found for unknown revision of a context" in {
      Get(s"/contexts/${contextId.show}?rev=5") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "reject updating a context when it is published" in {
      Put(s"/contexts/${contextId.show}?rev=3", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[CannotUpdatePublished.type]
      }
    }

    "deprecate a context" in {
      Delete(s"/contexts/${contextId.show}?rev=3") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual contextRefAsJson(ContextRef(contextId, 4L))
      }
      contexts.fetch(contextId).futureValue shouldEqual Some(
        Context(contextId, 4L, contextJson, deprecated = true, published = true))
    }

    "reject updating a context when it is deprecated" in {
      Put(s"/contexts/${contextId.show}?rev=4", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[ContextIsDeprecated.type]
      }
    }

    "reject the creation of schema importing non-existing context" in {
      val invalidContextId = ContextId(domRef.id, genString(length = 8), genVersion())
      val invalidContextJson = jsonContentOf(
        "/contexts/importing-context.json",
        Map(
          quote("{{org}}")   -> orgRef.id.id,
          quote("{{dom}}")   -> domRef.id.id,
          quote("{{mixed}}") -> genString(length = 8),
          quote("{{ver}}")   -> genVersion().show
        )
      )
      Put(s"/contexts/${invalidContextId.show}", invalidContextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalImportsViolation.type]
      }
    }

    "reject the creation of context from a deprecated domain" in {
      //Deprecate the domain
      doms.deprecate(domRef.id, domRef.rev)(caller).futureValue
      //Create a SchemaId from the deprecated domain
      val contextId2 = SchemaId(domRef.id, genString(length = 8), genVersion())

      Put(s"/contexts/${contextId2.show}", contextJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[DomainIsDeprecated.type]
      }
    }

  }
}

object ContextRoutesSpec {

  private def contextRefAsJson(ref: ContextRef) =
    Json.obj(
      "@context" -> Json.fromString(prefixes.CoreContext.toString),
      "@id"      -> Json.fromString(s"$baseUri/contexts/${ref.id.show}"),
      "nxv:rev"  -> Json.fromLong(ref.rev)
    )

  private def sparqlClient()(implicit cl: UntypedHttpClient[Future],
                             ec: ExecutionContext,
                             mt: Materializer): SparqlClient[Future] = {
    import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
    val sparqlUri = Uri("http://localhost:9999/bigdata/sparql")
    SparqlClient[Future](sparqlUri)

  }
}
