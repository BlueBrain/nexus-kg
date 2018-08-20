package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import org.scalatest.{EitherValues, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._

class PathDirectivesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with EitherValues {

  "A PathDirectives" should {
    val mappings = Map(
      "nxv" -> Iri.absolute("https://bluebrain.github.io/nexus/vocabulary/").right.value,
      "a"   -> Iri.absolute("https://www.w3.org/1999/02/22-rdf-syntax-ns#type").right.value
    )
    implicit val project: Project =
      Project("project",
              "some-label",
              mappings,
              Iri.absolute("http://example.com/base/").right.value,
              1L,
              false,
              "uuid")

    def route(): Route =
      (get & pathPrefix(IdSegment)) { iri =>
        complete(StatusCodes.OK -> iri.show)
      }

    def routeIsSegment(iri: AbsoluteIri): Route =
      (get & pathPrefix(isIdSegment(iri)) & pathPrefix(Segment) & pathEndOrSingleSlash) { s =>
        complete(StatusCodes.OK -> s)
      }

    "pass a route with a specific segment id" in {
      Get("/nxv:rev/other") ~> routeIsSegment(nxv.rev.value) ~> check {
        responseAs[String] shouldEqual "other"
      }
    }

    "do not pass a route when a specific segment id does not match" in {
      Get("/nxv:rev/other") ~> routeIsSegment(nxv.distribution.value) ~> check {
        handled shouldEqual false
      }
    }

    "expand a curie" in {
      Get("/nxv:rev/other") ~> route() ~> check {
        responseAs[String] shouldEqual "https://bluebrain.github.io/nexus/vocabulary/rev"
      }
    }

    "replace an alias" in {
      Get("/a") ~> route() ~> check {
        responseAs[String] shouldEqual "https://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      }
    }

    "matches an absoluteIri" in {
      Get("/c:d") ~> route() ~> check {
        responseAs[String] shouldEqual "c:d"
      }
    }

    "append the uuid to the base" in {
      Get("/uuid") ~> route() ~> check {
        responseAs[String] shouldEqual "http://example.com/base/uuid"
      }
    }

    "do not match a wrong iri" in {
      Get("/?nds?i=ad?dsd") ~> route() ~> check {
        handled shouldEqual false
      }
    }

  }

}
