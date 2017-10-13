package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.model.headers.{Location, `Content-Type`}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.routes.StaticRoutes.ServiceDescription
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

class StaticRoutesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with ScalaFutures {

  "A StaticRoutes" should {
    "return the correct service description" in {
      val expected = {
        val desc = Settings(system).Description
        ServiceDescription(desc.Name, desc.Version, desc.Environment)
      }

      Get("/") ~> StaticRoutes().routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[ServiceDescription] shouldEqual expected
      }
    }
    "redirect docs to docs/kg/index.html" in {
      Get("/docs") ~> StaticRoutes().routes ~> check {
        status shouldEqual StatusCodes.MovedPermanently
        response.header[Location].get.uri.path.toString shouldEqual "/docs/kg/index.html"
      }
    }
    "redirect docs/kg to docs/kg/" in {
      Get("/docs/kg") ~> StaticRoutes().routes ~> check {
        status shouldEqual StatusCodes.MovedPermanently
        response.header[Location].get.uri.path.toString shouldEqual "/docs/kg/"
      }
    }

    "return documentation/" in {
      Get("/docs/kg/") ~> StaticRoutes().routes ~> check {
        status shouldEqual StatusCodes.OK
        response.header[`Content-Type`].get.contentType shouldEqual ContentTypes.`text/html(UTF-8)`
      }
    }
  }
}
