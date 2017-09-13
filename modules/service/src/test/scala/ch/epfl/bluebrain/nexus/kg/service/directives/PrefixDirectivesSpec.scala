package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.service.directives.PrefixDirectives._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class PrefixDirectivesSpec extends WordSpecLike with Matchers with Inspectors with ScalatestRouteTest {

  "A PrefixDirective" should {

    "match the prefix uri" in {
      forAll(Map(
        ""         -> "",
        "/"        -> "",
        "///"      -> "",
        "/dev"     -> "/dev",
        "/dev/"    -> "/dev",
        "/dev///"  -> "/dev",
        "/dev/sn/" -> "/dev/sn"
      ).toList) { case (suffix, prefix) =>
        val uri = Uri("http://localhost:80" + suffix)
        val route = uriPrefix(uri) {
          path("remainder") {
            get {
              complete(StatusCodes.OK)
            }
          }
        }

        Get(prefix + "/remainder") ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      }
    }
  }
}
