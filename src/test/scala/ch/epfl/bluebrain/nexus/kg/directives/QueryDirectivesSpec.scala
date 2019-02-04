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

    def routeFormat(strict: Boolean, default: OutputFormat): Route =
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

    "return jsonLD format from Accept header and query params. on strict mode" in {
      Get("/some?format=compacted") ~> Accept(`application/json`) ~> routeFormat(strict = true, Compacted) ~> check {
        responseAs[String] shouldEqual "Compacted"
      }
      Get("/some?format=expanded") ~> Accept(`application/json`) ~> routeFormat(strict = true, Compacted) ~> check {
        responseAs[String] shouldEqual "Expanded"
      }
    }

    "ignore query param. and return default format when Accept header does not match on strict mode" in {
      Get("/some?format=expanded") ~> Accept(`application/*`) ~> routeFormat(strict = true, Binary) ~> check {
        responseAs[String] shouldEqual "Binary"
      }
      Get("/some?format=compacted") ~> Accept(`application/*`, `*/*`) ~> routeFormat(strict = true, DOT) ~> check {
        responseAs[String] shouldEqual "DOT"
      }
    }

    "return the format from the closest Accept header match and the query param" in {
      Get("/some?format=expanded") ~> Accept(`application/*`) ~> routeFormat(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "Expanded"
      }
      Get("/some") ~> Accept(`application/ntriples`, `*/*`) ~> routeFormat(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "Triples"
      }

      Get("/some") ~> Accept(`text/*`, `*/*`) ~> routeFormat(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "DOT"
      }

      Get("/some?format=compacted") ~> Accept(`application/javascript`,
                                              DOT.contentType.mediaType,
                                              `application/ntriples`,
                                              `*/*`) ~> routeFormat(strict = false, Binary) ~> check {
        responseAs[String] shouldEqual "DOT"
      }
    }
  }
}
