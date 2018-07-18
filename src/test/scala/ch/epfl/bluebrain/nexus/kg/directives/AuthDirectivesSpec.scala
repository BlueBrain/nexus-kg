package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, OAuth2BearerToken}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.Caller.{AnonymousCaller, AuthenticatedCaller}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types.Permission._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error
import ch.epfl.bluebrain.nexus.kg.Error._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.DownstreamServiceError
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, EitherValues, Matchers, WordSpecLike}

class AuthDirectivesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest {

  private implicit val client    = mock[AdminClient[Task]]
  private implicit val iamClient = mock[IamClient[Task]]
  private implicit val label     = ProjectLabel("uuidAccount", "uuidProject")
  private val readWrite          = Permissions(Read, Write)
  private val ownPublish         = Permissions(Own, Permission("publish"))

  before(Mockito.reset(client))

  "Authentication directives" should {

    def tokenRoute(): Route = {
      handleRejections(RejectionHandling.rejectionHandler()) {
        (get & token) { optToken =>
          complete(StatusCodes.OK -> optToken.map(_.value).getOrElse("empty"))
        }
      }
    }

    def permissionsRoute(perms: Permissions)(implicit token: Option[AuthToken]): Route = {
      import monix.execution.Scheduler.Implicits.global
      handleRejections(RejectionHandling.rejectionHandler()) {
        (get & hasPermission(perms)) {
          complete(StatusCodes.OK)
        }
      }
    }

    def identityRoute(implicit token: Option[AuthToken]): Route = {
      import monix.execution.Scheduler.Implicits.global
      handleRejections(RejectionHandling.rejectionHandler()) {
        (get & callerIdentity) { identity =>
          complete(StatusCodes.OK -> identity)
        }
      }
    }

    "reject token which is not bearer" in {
      Get("/") ~> addCredentials(BasicHttpCredentials("something")) ~> tokenRoute() ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "fetch token" in {
      Get("/") ~> addCredentials(OAuth2BearerToken("something")) ~> tokenRoute() ~> check {
        responseAs[String] shouldEqual "something"
      }
    }

    "fetch empty token" in {
      Get("/") ~> tokenRoute() ~> check {
        responseAs[String] shouldEqual "empty"
      }
    }

    "pass when the permissions are present" in {
      implicit val token: Option[AuthToken] = None
      val acls                              = FullAccessControlList((Anonymous, "some" / "path", readWrite))
      when(client.getProjectAcls(label.account, label.value, parents = true, self = true))
        .thenReturn(Task.pure(Some(acls)))
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        response.status shouldEqual StatusCodes.OK
      }
    }

    "reject when the permissions aren't present" in {
      implicit val token: Option[AuthToken] = None
      val acls                              = FullAccessControlList((Anonymous, "some" / "path", ownPublish))
      when(client.getProjectAcls(label.account, label.value, parents = true, self = true))
        .thenReturn(Task.pure(Some(acls)))
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when the underlying admin client fails with UnauthorizedAccess" in {
      implicit val token: Option[AuthToken] = None
      when(client.getProjectAcls(label.account, label.value, parents = true, self = true))
        .thenReturn(Task.raiseError(UnauthorizedAccess))
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when the underlying admin client fails with another error" in {
      implicit val token: Option[AuthToken] = None
      when(client.getProjectAcls(label.account, label.value, parents = true, self = true))
        .thenReturn(Task.raiseError(new RuntimeException()))
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
      }
    }

    "return the UserRef from the iam call" in {
      val token                                = AuthToken("val")
      implicit val optToken: Option[AuthToken] = Some(token)
      val user                                 = UserRef("realm", "dmontero")
      when(iamClient.getCaller(filterGroups = true)).thenReturn(Task.pure(AuthenticatedCaller(token, user, Set.empty)))
      Get("/") ~> identityRoute ~> check {
        responseAs[Identity] shouldEqual (user: Identity)
      }
    }

    "return Anonymous when anonymous caller is returned from the iam call" in {
      implicit val optToken: Option[AuthToken] = Some(AuthToken("val"))
      when(iamClient.getCaller(filterGroups = true)).thenReturn(Task.pure(AnonymousCaller))
      Get("/") ~> identityRoute ~> check {
        responseAs[Identity] shouldEqual (Anonymous: Identity)
      }
    }

    "reject when the underlying iam client fails with UnauthorizedAccess" in {
      implicit val token: Option[AuthToken] = None
      when(iamClient.getCaller(filterGroups = true)).thenReturn(Task.raiseError(UnauthorizedAccess))
      Get("/") ~> identityRoute ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when the underlying iam client fails with another error" in {
      implicit val token: Option[AuthToken] = None
      when(iamClient.getCaller(filterGroups = true)).thenReturn(Task.raiseError(new RuntimeException()))
      Get("/") ~> identityRoute ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
      }
    }
  }

}
