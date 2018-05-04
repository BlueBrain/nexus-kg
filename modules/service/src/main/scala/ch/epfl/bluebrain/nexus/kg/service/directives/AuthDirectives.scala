package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.javadsl.server.CustomRejection
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.refined.project.ProjectReference
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.{Permission, Permissions}
import ch.epfl.bluebrain.nexus.kg.core.access.{Access, HasAccess}
import ch.epfl.bluebrain.nexus.kg.core.rejections.CommonRejections
import ch.epfl.bluebrain.nexus.kg.core.rejections.CommonRejections.DownstreamServiceError
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceType
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.authorizationRejection

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait AuthDirectives {

  /**
    * Directives that checks if the caller has an ''access'' permission on the ''resourceType''
    * for the project ''name''.
    *
    * @param name the project name
    * @param resourceType the resource type
    * @param access the access right
    */
  def authorizeOn[T <: ResourceType, A <: Access](name: ProjectReference, resourceType: T, access: A)(
      implicit adminClient: AdminClient[Future],
      credentials: Option[OAuth2BearerToken]): Directive1[T HasAccess A] = {
    onComplete(adminClient.getProjectAcls(name, true, true)).flatMap {
      case Success(acls) =>
        if (acls.hasAnyPermission(Permissions(Permission(s"${resourceType.name}/${access.name}"))))
          provide(HasAccess[T, A]())
        else reject(AuthorizationFailedRejection)
      case Failure(UnauthorizedAccess) => reject(AuthorizationFailedRejection)
      case Failure(err)                => reject(authorizationRejection(err))
    }
  }
}

object AuthDirectives extends AuthDirectives {

  /**
    * Signals that the authentication was rejected with an unexpected error.
    *
    * @param err the [[CommonRejections]]
    */
  final case class CustomAuthRejection(err: CommonRejections) extends CustomRejection

  private[directives] def authorizationRejection(err: Throwable) =
    CustomAuthRejection(
      DownstreamServiceError(Try(err.getMessage).getOrElse("error while authenticating on the downstream service")))
}
