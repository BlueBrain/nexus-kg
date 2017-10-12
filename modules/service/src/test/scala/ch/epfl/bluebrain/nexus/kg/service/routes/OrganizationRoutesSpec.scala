package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection._
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations._
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization, Organizations}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.OrganizationRoutesSpec._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future

class OrganizationRoutesSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with ScalaFutures {

  "An OrganizationRoutes" should {
    val agg  = MemoryAggregate("orgs")(initial, next, eval).toF[Future]
    val orgs = Organizations(agg)

    val sparqlUri                  = Uri("http://localhost:9999/bigdata/sparql")
    val vocab                      = baseUri.copy(path = baseUri.path / "core")
    val querySettings              = QuerySettings(Pagination(0L, 20), "org-index", vocab)
    implicit val filteringSettings = FilteringSettings(vocab, vocab)
    implicit val cl                = HttpClient.akkaHttpClient

    val sparqlClient = SparqlClient[Future](sparqlUri)
    val route =
      OrganizationRoutes(orgs, sparqlClient, querySettings, baseUri).routes

    val id          = OrgId(genString(length = 3))
    val json        = Json.obj("key" -> Json.fromString(genString(length = 8)))
    val jsonUpdated = Json.obj("key" -> Json.fromString(genString(length = 8)))

    "create an organization" in {
      Put(s"/organizations/${id.show}", json) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 1L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 1L, json, deprecated = false))
    }

    "reject the creation of an organization with invalid id" in {
      Put(s"/organizations/invalidId!", json) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidOrganizationId.type]
      }
    }

    "reject the creation of an organization which already exists" in {
      Put(s"/organizations/${id.show}", json) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[OrgAlreadyExists.type]
      }
    }

    "update an organization" in {
      Put(s"/organizations/${id.show}?rev=1", jsonUpdated) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 2L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 2L, jsonUpdated, deprecated = false))
    }

    "reject updating an organization with incorrect rev" in {
      Put(s"/organizations/${id.show}?rev=10", jsonUpdated) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "reject updating an organization with wrong name" in {
      Put(s"/organizations/noexist?rev=2", jsonUpdated) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[OrgDoesNotExist.type]
      }
    }

    "return the current organization" in {
      Get(s"/organizations/${id.show}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json
          .obj("@id"        -> Json.fromString(s"$baseUri/organizations/${id.id}"),
               "rev"        -> Json.fromLong(2L),
               "deprecated" -> Json.fromBoolean(false))
          .deepMerge(jsonUpdated)
      }
    }

    "return not found for unknown organizations" in {
      Get(s"/organizations/${genString(length = 3)}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "deprecate an organization" in {
      Delete(s"/organizations/${id.show}?rev=2") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual orgRefAsJson(OrgRef(id, 3L))
      }
      orgs.fetch(id).futureValue shouldEqual Some(Organization(id, 3L, jsonUpdated, deprecated = true))
    }

    "reject the deprecation of an organization already deprecated" in {
      Delete(s"/organizations/${id.show}?rev=3") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[OrgIsDeprecated.type]
      }
    }
  }
}

object OrganizationRoutesSpec {
  private val baseUri = Uri("http://localhost/v0")

  private def orgRefAsJson(ref: OrgRef) = Json.obj(
    "@id" -> Json.fromString(s"$baseUri/organizations/${ref.id.id}"),
    "rev" -> Json.fromLong(ref.rev)
  )
}
