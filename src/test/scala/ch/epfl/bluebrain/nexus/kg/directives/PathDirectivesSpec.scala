package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project.LoosePrefixMapping
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import eu.timepit.refined.auto._
import org.scalatest.{Matchers, WordSpecLike}

class PathDirectivesSpec extends WordSpecLike with Matchers with ScalatestRouteTest {

  "A PathDirectives" should {
    implicit val mappings = List(LoosePrefixMapping("nxv", "https://bluebrain.github.io/nexus/vocabulary/"),
                                 LoosePrefixMapping("a", "https://www.w3.org/1999/02/22-rdf-syntax-ns#type"))

    def route(): Route =
      (get & pathPrefix(aliasOrCurie)) { iri =>
        complete(StatusCodes.OK -> iri.show)
      }

    "expand a curie" in {
      Get("/nxv:rev") ~> route() ~> check {
        responseAs[String] shouldEqual "https://bluebrain.github.io/nexus/vocabulary/rev"
      }
    }

    "replace an alias" in {
      Get("/a") ~> route() ~> check {
        responseAs[String] shouldEqual "https://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      }
    }

    "do not match unexisting prefix on curie" in {
      Get("/c:d") ~> route() ~> check {
        handled shouldEqual false
      }
    }

    "do not match unexisting alias" in {
      Get("/something") ~> route() ~> check {
        handled shouldEqual false
      }
    }

  }

}
