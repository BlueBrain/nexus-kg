package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.core.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.routes.StaticRoutes.ServiceDescription
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

class StaticRoutesSpec extends WordSpecLike with Matchers with ScalatestRouteTest with ScalaFutures {
  private val description = Settings(system).appConfig.description

  "A StaticRoutes" should {
    "return the correct service description" in {
      val expected = ServiceDescription(description.name, description.version)

      Get("/") ~> StaticRoutes(description).routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[ServiceDescription] shouldEqual expected
      }
    }
  }
}
