package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.AuthorizationFailedRejection
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.admin.refined.project.ProjectReference
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Permission, Permissions}
import ch.epfl.bluebrain.nexus.kg.core.access.Access._
import ch.epfl.bluebrain.nexus.kg.core.access.HasAccess
import ch.epfl.bluebrain.nexus.kg.core.rejections.CommonRejections.DownstreamServiceError
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceType._
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.{authorizeOn, CustomAuthRejection}
import ch.epfl.bluebrain.nexus.service.http.Path
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, Inspectors, Matchers, WordSpecLike}
import eu.timepit.refined.auto._

import scala.concurrent.Future

class AuthDirectivesSpec
    extends WordSpecLike
    with ScalatestRouteTest
    with Matchers
    with ScalaFutures
    with Randomness
    with BeforeAndAfter
    with Inspectors {

  private val cred: Option[OAuth2BearerToken] = Some(OAuth2BearerToken("token"))
  private val projectName: ProjectReference   = "projectname"

  "An AuthDirectives" should {
    "forward unauthorized status codes from the admin service" in {
      val cl = mockAdminClient(projectName, Future.failed(UnauthorizedAccess))

      Get("/") ~> route(cl) ~> check {
        rejection shouldEqual AuthorizationFailedRejection
        handled shouldEqual false
      }
    }

    "forward errors from the admin service" in {
      val cl = mockAdminClient(projectName, Future.failed(new RuntimeException("Error message")))

      Get("/") ~> route(cl) ~> check {
        rejection shouldEqual CustomAuthRejection(DownstreamServiceError("Error message"))
        handled shouldEqual false
      }
    }

    "return unauthorized when the requested permission is not found" in {
      val acls = FullAccessControlList((Anonymous(), Path./, Permissions(Permission("schema/write"))))
      val cl   = mockAdminClient(projectName, Future.successful(acls))

      Get("/") ~> route(cl) ~> check {
        rejection shouldEqual AuthorizationFailedRejection
        handled shouldEqual false
      }
    }

    "return the access right when the requested permission is found" in {
      val acls = FullAccessControlList((Anonymous(), Path./, Permissions(Permission("schema/read"))))
      val cl   = mockAdminClient(projectName, Future.successful(acls))

      Get("/") ~> route(cl) ~> check {
        responseAs[String] shouldEqual "OK"
        status shouldEqual StatusCodes.OK
      }
    }
  }

  private def route(cl: AdminClient[Future]) = {
    authorizeOn(projectName, Schema, Read)(cl, cred) { x =>
      x.isInstanceOf[HasAccess[SchemaType, Read]] shouldEqual true
      complete("OK")
    }
  }

  private def mockAdminClient(expectedName: ProjectReference,
                              expectedResponse: Future[FullAccessControlList]): AdminClient[Future] =
    new AdminClient[Future] {
      override def getProject(name: ProjectReference)(
          implicit credentials: Option[OAuth2BearerToken]): Future[Project] = ???

      override def getProjectAcls(name: ProjectReference, parents: Boolean, self: Boolean)(
          implicit credentials: Option[OAuth2BearerToken]): Future[FullAccessControlList] = {
        expectedName shouldEqual name
        parents shouldEqual true
        self shouldEqual true
        expectedResponse
      }
    }
}
