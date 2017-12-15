package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.service.prefixes.ErrorContext
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.{Error, ExceptionHandling, RejectionHandling}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.scalatest.{Matchers, WordSpecLike}

class ResourceDirectivesSpec extends WordSpecLike with ScalatestRouteTest with Matchers {

  private def route[A: Qualifier](dir: Directive1[A]): Route =
    (handleExceptions(ExceptionHandling.exceptionHandler(ErrorContext)) & handleRejections(
      RejectionHandling.rejectionHandler(ErrorContext))) {
      dir { extracted =>
        complete(extracted.qualifyAsStringWith(""))
      }
    }

  "A ResourceDirectives" should {
    "match with orgId" in {
      Get("/org") ~> route(extractOrgId) ~> check {
        responseAs[String] shouldEqual "/organizations/org"
      }
    }
    "match with domId" in {
      Get("/org/dom") ~> route(extractDomainId) ~> check {
        responseAs[String] shouldEqual "/domains/org/dom"
      }
    }
    "match with SchemaName" in {
      Get("/org/dom/name") ~> route(extractSchemaName) ~> check {
        responseAs[String] shouldEqual "/schemas/org/dom/name"
      }
    }
    "match with SchemaId" in {
      Get("/org/dom/name/v1.0.0") ~> route(extractSchemaId) ~> check {
        responseAs[String] shouldEqual "/schemas/org/dom/name/v1.0.0"
      }
    }
    "match with InstanceId" in {
      Get("/org/dom/name/v1.0.0/uuid") ~> route(extractInstanceId) ~> check {
        responseAs[String] shouldEqual "/data/org/dom/name/v1.0.0/uuid"
      }
    }
    "match with ContextName" in {
      Get("/org/dom/name") ~> route(extractContextName) ~> check {
        responseAs[String] shouldEqual "/contexts/org/dom/name"
      }
    }
    "match with ContextId" in {
      Get("/org/dom/name/v1.0.0") ~> route(extractContextId) ~> check {
        responseAs[String] shouldEqual "/contexts/org/dom/name/v1.0.0"
      }
    }
    "reject illegal version in InstanceId with an IllegalVersionFormat" in {
      Get("/org/dom/name/v1.0/uuid") ~> route(extractInstanceId) ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }
    "reject illegal version in SchemaId with an IllegalVersionFormat" in {
      Get("/org/dom/name/v1.0") ~> route(extractSchemaId) ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }
    "reject illegal version in ContextId with an IllegalVersionFormat" in {
      Get("/org/dom/name/v1.0") ~> route(extractContextId) ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
      }
    }
  }

}
