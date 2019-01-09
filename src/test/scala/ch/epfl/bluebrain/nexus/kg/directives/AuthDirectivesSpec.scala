package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, OAuth2BearerToken}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error._
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.DownstreamServiceError
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, EitherValues, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

class AuthDirectivesSpec
    extends WordSpecLike
    with Matchers
    with TestHelper
    with EitherValues
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest {

  private implicit val config    = IamClientConfig(url"http://nexus.example.com/iam/v1".value)
  private implicit val iamClient = mock[IamClient[Task]]
  private implicit val aclsOps   = mock[AclsOps]
  private implicit val label     = ProjectLabel("organizationLabel", "projectLabel")
  private val readWrite          = Set(Permission.unsafe("read"), Permission.unsafe("write"))
  private val ownPublish         = Set(Permission.unsafe("own"), Permission.unsafe("publish"))

  before {
    Mockito.reset(iamClient)
    Mockito.reset(aclsOps)
  }

  "Authentication directives" should {

    def tokenRoute(): Route = {
      handleRejections(RejectionHandling()) {
        (get & token) { optToken =>
          complete(StatusCodes.OK -> optToken.map(_.value).getOrElse("empty"))
        }
      }
    }

    def aclsRoute(): Route = {
      import monix.execution.Scheduler.Implicits.global
      handleRejections(RejectionHandling()) {
        (get & acls) { acl =>
          complete(StatusCodes.OK -> acl)
        }
      }
    }

    def permissionsRoute(perms: Set[Permission])(implicit
                                                 acls: AccessControlLists,
                                                 caller: Caller): Route = {
      handleRejections(RejectionHandling()) {
        (get & hasPermission(perms)) {
          complete(StatusCodes.OK)
        }
      }
    }

    def identityRoute(implicit token: Option[AuthToken]): Route = {
      import monix.execution.Scheduler.Implicits.global
      handleRejections(RejectionHandling()) {
        (get & caller) { implicit c =>
          complete(StatusCodes.OK -> (c.subject: Identity))
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

    "fetch acls" in {
      val acls = AccessControlLists("some" / "path" -> resourceAcls(AccessControlList(Anonymous -> ownPublish)))
      when(aclsOps.fetch()).thenReturn(Task.pure(acls))

      Get("/") ~> aclsRoute() ~> check {
        response.status shouldEqual StatusCodes.OK
        responseAs[AccessControlLists] shouldEqual acls
      }
    }

    "return UnauthorizedAccess fetching acls" in {
      when(aclsOps.fetch()).thenReturn(Task.raiseError(UnauthorizedAccess))

      Get("/") ~> aclsRoute() ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return unknown error fetching acls" in {
      when(aclsOps.fetch()).thenReturn(Task.raiseError(new RuntimeException()))

      Get("/") ~> aclsRoute() ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
      }
    }

    "pass when the permissions are present" in {
      implicit val acls =
        AccessControlLists(label.organization / label.value -> resourceAcls(AccessControlList(Anonymous -> readWrite)))
      implicit val caller: Caller = Caller.anonymous
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        response.status shouldEqual StatusCodes.OK
      }
    }

    "reject when the permissions aren't present" in {
      implicit val acls =
        AccessControlLists(label.organization / label.value -> resourceAcls(AccessControlList(Anonymous -> ownPublish)))
      implicit val caller: Caller = Caller.anonymous
      Get("/") ~> permissionsRoute(readWrite) ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "return the User from the iam call" in {
      val token                                = AuthToken("val")
      implicit val optToken: Option[AuthToken] = Some(token)
      val user                                 = User("dmontero", "realm")
      when(iamClient.identities).thenReturn(Task.pure(Caller(user, Set[Identity](user))))
      Get("/") ~> identityRoute ~> check {
        responseAs[Identity] shouldEqual (user: Identity)
      }
    }

    "return Anonymous when anonymous caller is returned from the iam call" in {
      implicit val optToken: Option[AuthToken] = Some(AuthToken("val"))
      when(iamClient.identities).thenReturn(Task.pure(Caller.anonymous))
      Get("/") ~> identityRoute ~> check {
        responseAs[Identity] shouldEqual (Anonymous: Identity)
      }
    }

    "reject when the underlying iam client fails with UnauthorizedAccess" in {
      implicit val token: Option[AuthToken] = None
      when(iamClient.identities).thenReturn(Task.raiseError(UnauthorizedAccess))
      Get("/") ~> identityRoute ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when the underlying iam client fails with another error" in {
      implicit val token: Option[AuthToken] = None
      when(iamClient.identities).thenReturn(Task.raiseError(new RuntimeException()))
      Get("/") ~> identityRoute ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
      }
    }
  }

}
