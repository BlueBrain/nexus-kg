package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{
  ScoredQueryResult,
  UnscoredQueryResult
}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.{
  ScoredQueryResults,
  UnscoredQueryResults
}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest._
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class OrgIntegrationSpec(apiUri: Uri, route: Route, vocab: Uri)(
    implicit
    as: ActorSystem,
    ec: ExecutionContextExecutor,
    mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, vocab) {

  import BootstrapIntegrationSpec._
  import orgsEncoder._

  "A OrganizationRoutes" when {

    "performing integration tests" should {

      "create organizations successfully" in {
        forAll(orgs) { orgId =>
          val json = Json.obj("key" -> Json.fromString(genString()))
          Put(s"/organizations/${orgId.show}", json) ~> route ~> check {
            status shouldEqual StatusCodes.Created
            responseAs[Json] shouldEqual OrgRef(orgId, 1L).asJson
          }
        }
      }

      "list organizations" in {
        eventually(timeout(Span(indexTimeout, Seconds)),
                   interval(Span(1, Seconds))) {
          Get(s"/organizations") ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults =
              UnscoredQueryResults(orgs.length.toLong, orgs.map {
                UnscoredQueryResult(_)
              })
            val expectedLinks = List(Link("self", s"$apiUri/organizations"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                           expectedLinks).asJson
          }
        }
      }

      "list organizations with pagination" in {
        val pagination = Pagination(1L, 1)
        val path =
          s"/organizations?from=${pagination.from}&size=${pagination.size}"

        eventually(timeout(Span(indexTimeout, Seconds)),
                   interval(Span(1, Seconds))) {
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults =
              UnscoredQueryResults(orgs.length.toLong,
                                   List(UnscoredQueryResult(orgs(1))))
            val expectedLinks =
              List(Link("self", s"$apiUri$path"),
                   Link("previous",
                        s"$apiUri$path".replace("from=1", "from=0")),
                   Link("next", s"$apiUri$path".replace("from=1", "from=2")))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                           expectedLinks).asJson
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path = s"/organizations?size=${pagination.size}&from=100"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            UnscoredQueryResults(orgs.length.toLong,
                                 List.empty[UnscoredQueryResult[OrgId]])
          val expectedLinks =
            List(Link("self", s"$apiUri$path"),
                 Link("previous",
                      s"$apiUri$path".replace("from=100", s"from=0")))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
      }

      "list organization with full text search" in {
        val path = s"/organizations?q=${orgs.head.id}"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, orgs.head)))
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val path = s"/organizations?q=${orgs.head.id}&size=3&from=200"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            ScoredQueryResults(1L, 1F, List.empty[ScoredQueryResult[OrgId]])
          val expectedLinks =
            List(Link("self", s"$apiUri$path"),
                 Link("previous",
                      s"$apiUri$path".replace("from=200", "from=0")))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
      }

      "list organizations with filter on 'path' organization " in {
        val uriFilter = URLEncoder.encode(
          s"""{"@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}, "filter": {"path": "${"organization".qualify}", "op": "eq", "value": "nexus"} } """,
          "UTF-8"
        )
        val path = s"/organizations?filter=$uriFilter"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            UnscoredQueryResults(1L, List(UnscoredQueryResult(orgs.head)))
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
      }

      "list organizations with deprecation" in {
        Get(s"/organizations?deprecated=false") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            UnscoredQueryResults(orgs.length.toLong, orgs.map {
              UnscoredQueryResult(_)
            })
          val expectedLinks =
            List(Link("self", s"$apiUri/organizations?deprecated=false"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
        Get(s"/organizations?deprecated=true") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults =
            UnscoredQueryResults(0, List.empty[UnscoredQueryResult[OrgId]])
          val expectedLinks =
            List(Link("self", s"$apiUri/organizations?deprecated=true"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults,
                                                         expectedLinks).asJson
        }
      }
    }
  }
}
