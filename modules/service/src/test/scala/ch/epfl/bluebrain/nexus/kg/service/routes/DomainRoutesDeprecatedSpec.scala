package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection._
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import io.circe.Json
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import DomainRoutesDeprecatedSpec._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import io.circe.syntax._

import scala.concurrent.Future

class DomainRoutesDeprecatedSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with ScalaFutures {

  "A DomainRoutesDeprecated" should {
    import domsEncoder._

    val orgAgg = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs   = Organizations(orgAgg)
    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Future]
    val doms = Domains(domAgg, orgs)

    val orgId       = OrgId(genString(length = 5))
    val id          = DomainId(orgId, genString(length = 8))
    val description = genString(length = 32)
    val json        = Json.obj("description" -> Json.fromString(description))

    orgs
      .create(orgId, Json.obj("key" -> Json.fromString(genString())))
      .futureValue

    val sparqlUri                  = Uri("http://localhost:9999/bigdata/sparql")
    val vocab                      = baseUri.copy(path = baseUri.path / "core")
    val querySettings              = QuerySettings(Pagination(0L, 20), "domain-index", vocab, baseUri)
    implicit val filteringSettings = FilteringSettings(vocab, vocab)
    implicit val cl                = HttpClient.akkaHttpClient

    val sparqlClient = SparqlClient[Future](sparqlUri)
    val route =
      DomainRoutesDeprecated(doms, sparqlClient, querySettings, baseUri).routes

    "create a domain" in {
      Put(s"/organizations/${orgId.show}/domains/${id.id}", json) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual DomainRef(id, 1L).asJson
      }
      doms.fetch(id).futureValue shouldEqual Some(Domain(id, 1L, deprecated = false, description))
    }

    "reject the creation of a domain which already exists" in {
      Put(s"/organizations/${orgId.show}/domains/${id.id}", json) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[DomainAlreadyExists.type]
      }
    }

    "reject the creation of a domain with wrong id" in {
      Put(s"/organizations/${orgId.show}/domains/${id.copy(id = "NotValid").id}", json) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InvalidDomainId.type]
      }
    }

    "return the current domain" in {
      Get(s"/organizations/${orgId.show}/domains/${id.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Domain(id, 1L, false, description).asJson
      }
    }

    "return not found for missing domain" in {
      Get(s"/organizations/${orgId.show}/domains/${id.id}-missing") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return not found for missing organization" in {
      Get(s"/organizations/${orgId.show}-missing/domains/${id.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "reject the deprecation of a domain with incorrect rev" in {
      Delete(s"/organizations/${orgId.show}/domains/${id.id}?rev=10", json) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "deprecate a domain" in {
      Delete(s"/organizations/${orgId.show}/domains/${id.id}?rev=1") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual DomainRef(id, 2L).asJson
      }
      doms.fetch(id).futureValue shouldEqual Some(Domain(id, 2L, deprecated = true, description))
    }

    "reject the deprecation of a domain already deprecated" in {
      Delete(s"/organizations/${orgId.show}/domains/${id.id}?rev=2", json) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[DomainAlreadyDeprecated.type]
      }
    }

    "reject the deprecation of a domain which does not exists" in {
      Delete(s"/organizations/${orgId.show}/domains/${id.copy(id = "something").id}?rev=1", json) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[DomainDoesNotExist.type]
      }
    }
  }
}

object DomainRoutesDeprecatedSpec {
  private val baseUri      = Uri("http://localhost/v0")
  implicit val domsEncoder = new DomainCustomEncoders(baseUri)
}
