package ch.epfl.bluebrain.nexus.kg.test

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, DomainRef}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest._
import org.scalatest.time.{Seconds, Span}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._

import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class DomainIntegrationSpec(apiUri: Uri, route: Route, vocab: Uri)(implicit
  as: ActorSystem, ec: ExecutionContextExecutor, mt: ActorMaterializer)
  extends BootstrapIntegrationSpec(apiUri, vocab) {

  import BootstrapIntegrationSpec._, domsEncoder._

  "A DomainRoutes" when {

    "performing integration tests" should {

      "create domains successfully" in {
        forAll(domains) { case domainId@DomainId(orgId, name) =>
          val jsonDomain = Json.obj("description" -> Json.fromString(s"$name-description"))
          Put(s"/organizations/${orgId.id}/domains/${name}", jsonDomain) ~> route ~> check {
            status shouldEqual StatusCodes.Created
            responseAs[Json] shouldEqual DomainRef(domainId, 1L).asJson
          }
        }
      }

      "list domains on organization nexus" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/organizations/nexus/domains") ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(5L, domains.filter(_.orgId.id == "nexus").map(UnscoredQueryResult(_)))
            val expectedLinks = List(Link("self", s"$apiUri/organizations/nexus/domains"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "list domains on organization rand with pagination" in {
        val pagination = Pagination(0L, 5)
        val path = s"/organizations/rand/domains?size=${pagination.size}"

        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(randDomains.length.toLong, randDomains.map(UnscoredQueryResult(_)).take(pagination.size))
            val expectedLinks = List(Link("self", s"$apiUri$path"), Link("next", s"$apiUri$path&from=5"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path = s"/organizations/rand/domains?size=${pagination.size}&from=100"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = UnscoredQueryResults(randDomains.length.toLong, List.empty[UnscoredQueryResult[DomainId]])
          val expectedLinks = List(Link("self", s"$apiUri$path"), Link("previous", s"$apiUri$path".replace("from=100", s"from=${randDomains.length - 5}")))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list domains on organization rand with full text search" in {
        val domainId = domains(7)
        val path = s"/organizations/rand/domains?q=${domainId.id}"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, domainId)))
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val path = s"/organizations/rand/domains?q=${domains(7).id}&size=3&from=200"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = ScoredQueryResults(1L, 1F, List.empty[ScoredQueryResult[DomainId]])
          val expectedLinks = List(Link("self", s"$apiUri$path"), Link("previous", s"$apiUri$path".replace("from=200", "from=0")))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list domains with filter of description" in {
        val domainId = domains(7)
        val uriFilter = URLEncoder.encode(s"""{"@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}, "filter": {"path": "${"description".qualify}", "op": "eq", "value": "${descriptionOf(domainId)}"} } """, "UTF-8")
        val path = s"/organizations/rand/domains?filter=$uriFilter"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(domainId)))
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "deprecate some domains on nexus organization" in {
        forAll(domains.take(2)) { domainId =>
          Delete(s"/organizations/nexus/domains/${domainId.id}?rev=1") ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual DomainRef(domainId, 2L).asJson
          }
        }
      }

      "list domains on organization nexus and deprecated" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/organizations/nexus/domains?deprecated=false") ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(3L, domains.slice(2, 5).map(UnscoredQueryResult(_)))
            val expectedLinks = List(Link("self", s"$apiUri/organizations/nexus/domains?deprecated=false"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }
    }
  }

  private def descriptionOf(domainId: DomainId): String = s"${domainId.id}-description"
}
