package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import io.circe.generic.auto._
import org.scalatest.{Matchers, WordSpecLike}
import cats.instances.future._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.routes.SparqlFixtures._
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchRoutesSpec._

import scala.concurrent.Future

class SearchRoutesSpec extends WordSpecLike with ScalatestRouteTest with Matchers {

  private val dataIndex = "data-index"

  private val sparqlUri = Uri("http://127.0.0.1:9999/bigdata/sparql")
  private implicit val mt = ActorMaterializer()
  private implicit val ec = system.dispatcher

  "A SearchRoutes" should {
    val pagination = Pagination(0L, 20)
    implicit val client: UntypedHttpClient[Future] = fixedHttpClient(fixedResponse("/full-text-search-sparql-result.json"))
    val sparqlClient = SparqlClient[Future](sparqlUri)
    val querySettings = QuerySettings(pagination, dataIndex, baseUri.copy(path = baseUri.path / "core"))
    val routes = SearchRoutes(sparqlClient, baseUri, querySettings).routes

    "return the appropriate query results" when {

      "performing a full text search" in {
        Get("/data?q=another") ~> routes ~> check {
          val expected = fixedListSchemas(Uri("http://example.com/data?q=another"))
          status shouldEqual StatusCodes.OK
          responseAs[Results] shouldEqual expected
        }
      }

      "performing a full text search with specific pagination" in {
        val specificPagination = Pagination(0L, 2)
        Get(s"/data?q=another&from=${specificPagination.from}&size=${specificPagination.size}") ~> routes ~> check {
          val expected = fixedListSchemas(Uri(s"http://example.com/data?q=another&from=${specificPagination.from}&size=${specificPagination.size}"))
          status shouldEqual StatusCodes.OK
          responseAs[Results] shouldEqual expected
        }
      }
    }
  }
}

object SearchRoutesSpec {
  final case class Result(resultId: String, score: Float, source: Source)

  final case class Results(total: Long, maxScore: Float, results: List[Result], links: List[Link])

  private val baseUri = Uri("http://127.0.0.1:8080/v0")

  private def fixedListSchemas(uri: Uri) =
    Results(0L, 1F, List(
      Result(
        s"$baseUri/data/org/domain/subject/v1.0.0/e973d182-373e-414a-a84f-dd9750636b27",
        0.625F,
        Source(
          s"$baseUri/data/org/domain/subject/v1.0.0/e973d182-373e-414a-a84f-dd9750636b27",
          List(Link("self", s"$baseUri/data/org/domain/subject/v1.0.0/e973d182-373e-414a-a84f-dd9750636b27"), Link("schema", s"$baseUri/schemas/org/domain/subject/v1.0.0")))),
      Result(
        s"$baseUri/data/org/domain/subject/v1.0.0/276a27c1-ae6e-4f99-baca-161fcb11e770",
        0.5F,
        Source(
          s"$baseUri/data/org/domain/subject/v1.0.0/276a27c1-ae6e-4f99-baca-161fcb11e770",
          List(Link("self", s"$baseUri/data/org/domain/subject/v1.0.0/276a27c1-ae6e-4f99-baca-161fcb11e770"), Link("schema", s"$baseUri/schemas/org/domain/subject/v1.0.0")))),
    ), List(Link("self", uri)))
}