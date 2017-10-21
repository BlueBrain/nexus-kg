package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.auth.types.Permission._
import ch.epfl.bluebrain.nexus.kg.auth.types.{AccessControlList, Permission}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.ResourceAccess.IamUri
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class ResourceAccessSpec
    extends WordSpecLike
    with ScalatestRouteTest
    with Matchers
    with ScalaFutures
    with Randomness
    with MockedIAMClient {

  private implicit val ec                                        = system.dispatcher
  private implicit val mt: ActorMaterializer                     = ActorMaterializer()
  private implicit val rs: HttpClient[Future, AccessControlList] = HttpClient.withAkkaUnmarshaller[AccessControlList]
  private implicit val iamUri                                    = IamUri("http://localhost:8080")

  private def route(cred: OAuth2BearerToken, perm: Permission) = {
    (handleExceptions(ExceptionHandling.exceptionHandler) & handleRejections(RejectionHandling.rejectionHandler)) {
      authorizeAsync(ResourceAccess.check(cred, DomainId(OrgId(s"org-${genString()}"), s"dom-${genString()}"), perm)) {
        complete("Success")
      }
    }
  }

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  "A ResourceAccessDirectives" should {
    "return unathorized whenever the token is wrong" in {
      Get("/organizations/org") ~> route(OAuth2BearerToken("invalidToken"), Read) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return unathorized when the requested path does not contains random permission" in {
      Get("/organizations/org/config") ~> route(OAuth2BearerToken(ValidToken), Permission("random")) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return authorized when the requested path contains read permission" in {
      Get("/organizations/org") ~> route(OAuth2BearerToken(ValidToken), Read) ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "Success"
      }
    }
  }
}
