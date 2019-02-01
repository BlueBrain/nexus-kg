package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PaginationConfig
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.{Compacted, Expanded}
import io.circe.generic.auto._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class QueryDirectivesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with EitherValues {

  "A QueryDirectives" should {
    implicit val config = PaginationConfig(0L, 10, 50)

    def route(): Route =
      (get & paginated) { page =>
        complete(StatusCodes.OK -> page)
      }

    def routeOutput(default: OutputFormat = Compacted): Route =
      (get & outputFormat(default)) { output =>
        complete(StatusCodes.OK -> output.name)
      }

    "return default values when no query parameters found" in {
      Get("/") ~> route() ~> check {
        responseAs[Pagination] shouldEqual Pagination(config.from, config.size)
      }
    }

    "return pagination from query parameters" in {
      Get("/some?from=1&size=20") ~> route() ~> check {
        responseAs[Pagination] shouldEqual Pagination(1L, 20)
      }
    }

    "return default parameters when the query params are under the minimum" in {
      Get("/some?from=-1&size=-1") ~> route() ~> check {
        responseAs[Pagination] shouldEqual Pagination(config.from, 1)
      }
    }

    "return default size when size is over the maximum" in {
      Get("/some?size=500") ~> route() ~> check {
        responseAs[Pagination] shouldEqual Pagination(config.from, config.sizeLimit)
      }
    }

    "return compacted output from query parameters" in {
      Get("/some?output=compacted") ~> routeOutput() ~> check {
        responseAs[String] shouldEqual Compacted.name
      }
    }

    "return expanded output from query parameters" in {
      Get("/some?output=expanded") ~> routeOutput() ~> check {
        responseAs[String] shouldEqual Expanded.name
      }
    }

    "return default output" in {
      Get("/some") ~> routeOutput(OutputFormat.Expanded) ~> check {
        responseAs[String] shouldEqual Expanded.name
      }
    }

    "fail on wrong output" in {
      Get("/some?output=not_exists") ~> routeOutput() ~> check {
        status shouldEqual StatusCodes.InternalServerError
      }
    }
  }
}
