package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.MediaRanges._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes._
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PaginationConfig
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import io.circe.generic.auto._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class QueryDirectivesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with EitherValues {

  "A QueryDirectives" should {
    implicit val config = PaginationConfig(0L, 10, 50)

    def route(): Route =
      (get & paginated) { page =>
        complete(StatusCodes.OK -> page)
      }

    def routeOutput(strict: Boolean, default: OutputFormat): Route =
      (get & outputFormat(strict, default)) { output =>
        complete(StatusCodes.OK -> output.toString)
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

    "return jsonLD output from Accept header and query params. on strict mode" in {
      Get("/some?output=compacted") ~> Accept(`application/json`) ~> routeOutput(strict = true, Compacted) ~> check {
        responseAs[String] shouldEqual "Compacted"
      }
      Get("/some?output=expanded") ~> Accept(`application/json`) ~> routeOutput(strict = true, Compacted) ~> check {
        responseAs[String] shouldEqual "Expanded"
      }
    }

    "ignore query param. and return default output when Accept header does not match on strict mode" in {
      Get("/some?output=expanded") ~> Accept(`application/*`) ~> routeOutput(strict = true, Binary) ~> check {
        responseAs[String] shouldEqual "Binary"
      }
      Get("/some?output=compacted") ~> Accept(`application/*`, `*/*`) ~> routeOutput(strict = true, DOT) ~> check {
        responseAs[String] shouldEqual "DOT"
      }
    }

    "return the output format from the closest Accept header match and the query param" in {
      Get("/some?output=expanded") ~> Accept(`application/*`) ~> routeOutput(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "Expanded"
      }
      Get("/some") ~> Accept(`application/ntriples`, `*/*`) ~> routeOutput(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "Triples"
      }

      Get("/some") ~> Accept(`text/*`, `*/*`) ~> routeOutput(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "DOT"
      }

      Get("/some?output=compacted") ~> Accept(`application/javascript`,
                                              DOT.contentType.mediaType,
                                              `application/ntriples`,
                                              `*/*`) ~> routeOutput(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "DOT"
      }
    }
  }
}
