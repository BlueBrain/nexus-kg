package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.{AnonymousCaller, AuthenticatedCaller}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.{Error, ExceptionHandling, MockedIAMClient, RejectionHandling}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class AuthDirectivesSpec
    extends WordSpecLike
    with ScalatestRouteTest
    with Matchers
    with ScalaFutures
    with Randomness
    with MockedIAMClient {

  private implicit val credOptionEncoder: Encoder[OAuth2BearerToken] =
    Encoder.encodeString.contramap {
      case OAuth2BearerToken(token) => s"$token"
    }

  private implicit val ec     = system.dispatcher
  private implicit val config = Configuration.default.withDiscriminator("type")
  private implicit val mt     = ActorMaterializer()
  implicit val cl             = iamClient("http://localhost:8080")

  private def route(perm: Permission)(implicit cred: Option[OAuth2BearerToken]) = {
    (handleExceptions(ExceptionHandling.exceptionHandler) & handleRejections(RejectionHandling.rejectionHandler)) {
      (get & authorizeResource(DomainId(OrgId(s"org-${genString()}"), s"dom-${genString()}"), perm)) {
        complete("Success")
      }
    }
  }

  private def route()(implicit cred: Option[OAuth2BearerToken]) = {
    (handleExceptions(ExceptionHandling.exceptionHandler) & handleRejections(RejectionHandling.rejectionHandler)) {
      (get & authenticateCaller) { caller =>
        complete(caller.asJson)
      }
    }
  }
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  "An AuthorizationDirectives" should {

    "return unathorized when the request contains an invalid token" in {
      implicit val cred: Option[OAuth2BearerToken] = Some(OAuth2BearerToken("invalid"))
      Get("/organizations/org") ~> route() ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return an authenticated caller when the request contains a valid token" in {
      implicit val cred: Option[OAuth2BearerToken] = Some(ValidCredentials)
      Get("/organizations/org") ~> route() ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual (AuthenticatedCaller(ValidCredentials, mockedUser): Caller).asJson
      }
    }

    "return an anonymous caller when the request contains a valid token" in {
      implicit val cred: Option[OAuth2BearerToken] = None
      Get("/organizations/org") ~> route() ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual (AnonymousCaller: Caller).asJson
      }
    }

    "return unathorized when the requested path does not contains random permission" in {
      implicit val cred: Option[OAuth2BearerToken] = Some(OAuth2BearerToken(ValidToken))
      Get("/organizations/org/config") ~> route(Permission("random")) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return unathorized when the token on the cred is wrong" in {
      implicit val cred: Option[OAuth2BearerToken] = Some(OAuth2BearerToken("Invalid"))
      Get("/organizations/org") ~> route(Read) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return authorized when the requested path for the authenticated cred contains read permission" in {
      implicit val cred: Option[OAuth2BearerToken] = Some(OAuth2BearerToken(ValidToken))
      Get("/organizations/org") ~> route(Read) ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "Success"
      }
    }

    "return authorized when the requested path for the anon cerd contains read permission" in {
      implicit val cred: Option[OAuth2BearerToken] = None
      Get("/organizations/org") ~> route(Read) ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "Success"
      }
    }
  }
}
