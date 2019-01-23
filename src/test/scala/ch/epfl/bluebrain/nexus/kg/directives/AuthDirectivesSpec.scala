package ch.epfl.bluebrain.nexus.kg.directives

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.iam.client.{IamClient, IamClientError}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.rdf.Iri
import monix.eval.Task
import org.mockito.matchers.MacroBasedMatchers
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, EitherValues, Matchers, WordSpecLike}

//noinspection NameBooleanParameters
class AuthDirectivesSpec
    extends WordSpecLike
    with Matchers
    with TestHelper
    with EitherValues
    with MacroBasedMatchers
    with IdiomaticMockito
    with BeforeAndAfter
    with ScalatestRouteTest {

  private implicit val iamClient: IamClient[Task] = mock[IamClient[Task]]
  private implicit val project: Project = Project(genIri,
                                                  "projectLabel",
                                                  "organizationLabel",
                                                  None,
                                                  genIri,
                                                  genIri,
                                                  Map.empty,
                                                  genUUID,
                                                  genUUID,
                                                  1L,
                                                  false,
                                                  Instant.EPOCH,
                                                  genIri,
                                                  Instant.EPOCH,
                                                  genIri)
  private val readWrite = Set(Permission.unsafe("read"), Permission.unsafe("write"))
//  private val ownPublish = Set(Permission.unsafe("own"), Permission.unsafe("publish"))

  before {
    Mockito.reset(iamClient)
  }

  "The AuthDirectives" should {
    "extract the token" in {
      val expected = "token"
      val route = extractToken {
        case Some(AuthToken(`expected`)) => complete("")
        case Some(_)                     => fail("Token was not extracted correctly.")
        case None                        => fail("Token was not extracted.")
      }
      Get("/").addCredentials(OAuth2BearerToken(expected)) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
    "extract no token" in {
      val route = extractToken {
        case None        => complete("")
        case t @ Some(_) => fail(s"Extracted unknown token '$t'.")
      }
      Get("/") ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "fail the route" when {
      "there are no permissions" in {
        implicit val acls: AccessControlLists = AccessControlLists()
        implicit val caller: Caller           = Caller(Anonymous, Set(Anonymous))
        val route = hasPermissions(readWrite).apply {
          complete("")
        }
        Get("/") ~> route ~> check {
          status shouldEqual StatusCodes.InternalServerError
        }
      }
      "the client throws an error for caller acls" in {
        implicit val token: Option[AuthToken] = None
        iamClient.acls(any[Iri.Path], true, true)(any[Option[AuthToken]]) shouldReturn Task.raiseError(
          IamClientError.UnknownError(StatusCodes.InternalServerError, ""))
        val route = callerAcls.apply { _ =>
          complete("")
        }
        Get("/") ~> route ~> check {
          status shouldEqual StatusCodes.InternalServerError
        }
      }
    }
  }
}
