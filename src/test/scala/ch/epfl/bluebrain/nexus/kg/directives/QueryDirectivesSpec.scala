package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PaginationConfig
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import io.circe.generic.auto._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class QueryDirectivesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with EitherValues {

  "A QueryDirectives" should {
    implicit val config = PaginationConfig(0L, 10, 50)

    def route(): Route =
      (get & paginated) { page =>
        complete(StatusCodes.OK -> page)
      }

    "return default values when no query parameters found" in {
      Get("/") ~> route() ~> check {
        responseAs[Pagination] shouldEqual Pagination(config.from, config.size)
      }
    }

    "return pagination form query parameters" in {
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

  }
}
