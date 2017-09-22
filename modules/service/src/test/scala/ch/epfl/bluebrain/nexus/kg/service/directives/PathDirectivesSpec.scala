package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.service.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.{Error, ExceptionHandling, RejectionHandling}
import org.scalatest.{Matchers, WordSpecLike}
import shapeless.Poly1
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._

class PathDirectivesSpec extends WordSpecLike with ScalatestRouteTest with Matchers {

  object completeWithType extends Poly1 {
    implicit def caseNone: Case.Aux[None.type, Route] = at[None.type] { _ =>
      complete("none")
    }
    implicit def caseDefault[A: Qualifier]: Case.Aux[A, Route] = at[A] { a =>
      complete(a.qualifyAsStringWith(""))
    }
  }

  private val route = {
    (handleExceptions(ExceptionHandling.exceptionHandler) & handleRejections(RejectionHandling.rejectionHandler)) {
      extractAnyResourceId() { id =>
        id.fold(completeWithType)
      }
    }
  }

  "An extractResourceId directive" should {
    "match with None for /" in {
      Get("/") ~> route ~> check {
        responseAs[String] shouldEqual "none"
      }
    }
    "match with OrgId" in {
      Get("/org") ~> route ~> check {
        responseAs[String] shouldEqual "/organizations/org"
      }
    }
    "match with DomainId" in {
      Get("/org/dom") ~> route ~> check {
        responseAs[String] shouldEqual "/organizations/org/domains/dom"
      }
    }
    "match with SchemaName" in {
      Get("/org/dom/name") ~> route ~> check {
        responseAs[String] shouldEqual "/schemas/org/dom/name"
      }
    }
    "match with SchemaId" in {
      Get("/org/dom/name/v1.0.0") ~> route ~> check {
        responseAs[String] shouldEqual "/schemas/org/dom/name/v1.0.0"
      }
    }
    "match with InstanceId" in {
      Get("/org/dom/name/v1.0.0/uuid") ~> route ~> check {
        responseAs[String] shouldEqual "/data/org/dom/name/v1.0.0/uuid"
      }
    }
    "reject the request with an IllegalVersionFormat" in {
      Get("/org/dom/name/v1.0/uuid") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }
  }

}
