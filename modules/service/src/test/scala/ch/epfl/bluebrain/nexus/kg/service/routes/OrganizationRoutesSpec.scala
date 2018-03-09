package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
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
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection._
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations._
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization, Organizations}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.linksEncoder
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.OrganizationRoutesSpec._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future

class OrganizationRoutesSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with ScalaFutures
    with MockedIAMClient {

  "An OrganizationRoutes" should {
    val agg               = MemoryAggregate("orgs")(initial, next, eval).toF[Future]
    val orgs              = Organizations(agg)
    val domAgg            = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms              = Domains(domAgg, orgs)
    val ctxAgg            = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
    implicit val contexts = Contexts(ctxAgg, doms, baseUri.toString())

    val sparqlUri                      = Uri("http://localhost:9999/bigdata/sparql")
    val vocab                          = baseUri.copy(path = baseUri.path / "core")
    val querySettings                  = QuerySettings(Pagination(0L, 20), 100, "org-index", vocab, baseUri)
    implicit val cl: IamClient[Future] = iamClient("http://localhost:8080")
    implicit val clock: Clock          = Clock.systemUTC
    implicit val tracing               = TracingDirectives()
    val caller                         = CallerCtx(clock, AnonymousCaller(Anonymous()))

    val indexingSettings   = ElasticIndexingSettings("", "", sparqlUri, sparqlUri)
    val sparqlClient       = SparqlClient[Future](sparqlUri)
    val elasticQueryClient = ElasticQueryClient[Future](sparqlUri)

    val elasticClient = ElasticClient[Future](sparqlUri, elasticQueryClient)
    val route =
      OrganizationRoutes(orgs, sparqlClient, elasticClient, indexingSettings, querySettings, baseUri).routes

    val id          = OrgId(genString(length = 3))
    val json        = Json.obj("key" -> Json.fromString(genString(length = 8)))
    val jsonUpdated = Json.obj("key" -> Json.fromString(genString(length = 8)))

    createNexusContexts(orgs, doms, contexts)(caller)

    "create an organization" in {
      Put(s"/organizations/${id.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 1L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 1L, json, deprecated = false))
    }

    "reject the creation of an organization with invalid id" in {
      Put(s"/organizations/invalidId!", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidOrganizationId.type]
      }
    }

    "reject the creation of an organization which already exists" in {
      Put(s"/organizations/${id.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[OrgAlreadyExists.type]
      }
    }

    "update an organization" in {
      Put(s"/organizations/${id.show}?rev=1", jsonUpdated) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 2L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 2L, jsonUpdated, deprecated = false))
    }

    "reject updating an organization with incorrect rev" in {
      Put(s"/organizations/${id.show}?rev=10", jsonUpdated) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "reject updating an organization with wrong name" in {
      Put(s"/organizations/noexist?rev=2", jsonUpdated) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[OrgDoesNotExist.type]
      }
    }

    "return the current organization" in {
      Get(s"/organizations/${id.show}") ~> addCredentials(ValidCredentials) ~> route ~> check {
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        responseAs[Json] shouldEqual Json
          .obj(
            "@id"      -> Json.fromString(s"$baseUri/organizations/${id.id}"),
            "@context" -> Json.fromString(prefixes.CoreContext.toString),
            "nxv:rev"  -> Json.fromLong(2L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/organizations/${id.id}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false)
          )
          .deepMerge(jsonUpdated)
      }
    }

    "return the current organization with a custom format" in {
      Get(s"/organizations/${id.show}?format=expanded") ~> addCredentials(ValidCredentials) ~> route ~> check {
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        status shouldEqual StatusCodes.OK
      }
    }

    "fetch old revision of an organization" in {
      Get(s"/organizations/${id.show}?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
        responseAs[Json] shouldEqual Json
          .obj(
            "@context" -> Json.fromString(prefixes.CoreContext.toString),
            "@id"      -> Json.fromString(s"$baseUri/organizations/${id.id}"),
            "nxv:rev"  -> Json.fromLong(1L),
            "links" -> Links("@context" -> s"${prefixes.LinksContext}",
                             "self" -> Uri(s"$baseUri/organizations/${id.id}")).asJson,
            "nxv:deprecated" -> Json.fromBoolean(false)
          )
          .deepMerge(json)
      }
    }

    "return not found for unknown organizations" in {
      Get(s"/organizations/${genString(length = 3)}") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return not found for unknown revision of an organization" in {
      Get(s"/organizations/${id.show}?rev=4") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "deprecate an organization" in {
      Delete(s"/organizations/${id.show}?rev=2") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 3L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 3L, jsonUpdated, deprecated = true))
    }

    "reject the deprecation of an organization already deprecated" in {
      Delete(s"/organizations/${id.show}?rev=3") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[OrgIsDeprecated.type]
      }
    }
  }
}

object OrganizationRoutesSpec {

  private def orgRefAsJson(ref: OrgRef) = Json.obj(
    "@context" -> Json.fromString(prefixes.CoreContext.toString),
    "@id"      -> Json.fromString(s"$baseUri/organizations/${ref.id.id}"),
    "nxv:rev"  -> Json.fromLong(ref.rev)
  )
}
