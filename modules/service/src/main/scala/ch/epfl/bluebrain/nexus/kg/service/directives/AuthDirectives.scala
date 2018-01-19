package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.javadsl.server.CustomRejection
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives.pass
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, _}
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.CustomAuthorizationRejection
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.DownstreamServiceError
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait AuthDirectives {

  private val prefix = Path("kg")

  /**
    * Checks if the caller associated with ''cred'' has permissions ''perms'' on the path ''resource''.
    *
    * @param resource the path which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource(resource: Path, perm: Permission)(implicit iamClient: IamClient[Future],
                                                          cred: Option[OAuth2BearerToken]): Directive0 =
    onComplete(iamClient.getAcls(prefix ++ resource)).flatMap {
      case Success(acls) if (acls.permissions.contains(perm)) => pass
      case Success(_)                                         => reject(AuthorizationFailedRejection)
      case Failure(UnauthorizedAccess)                        => reject(AuthorizationFailedRejection)
      case Failure(err)                                       => reject(authorizationRejection(err))
    }

  /**
    * Checks if the caller associated with ''cred'' has permissions ''perms'' on the resource ''resource''.
    *
    * @param resource the resource id which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource[Id](resource: Id, perm: Permission)(implicit iamClient: IamClient[Future],
                                                            cred: Option[OAuth2BearerToken],
                                                            S: Show[Id]): Directive0 =
    authorizeResource(Path(resource.show), perm)

  /**
    * Authenticates the requested with the provided ''cred'' and returns the ''caller''
    *
    * @return the [[Caller]]
    */
  def authenticateCaller(implicit iamClient: IamClient[Future], cred: Option[OAuth2BearerToken]): Directive1[Caller] =
    onComplete(iamClient.getCaller(cred, filterGroups = true)).flatMap {
      case Success(caller)             => provide(caller)
      case Failure(UnauthorizedAccess) => reject(AuthorizationFailedRejection)
      case Failure(err)                => reject(authorizationRejection(err))
    }

}

object AuthDirectives extends AuthDirectives {

  /**
    * Signals that the authorization was rejected with an unexpected error.
    *
    * @param err the [[CommonRejections]]
    */
  final case class CustomAuthorizationRejection(err: CommonRejections) extends CustomRejection

  private[directives] def authorizationRejection(err: Throwable): CustomAuthorizationRejection =
    CustomAuthorizationRejection(
      DownstreamServiceError(Try(err.getMessage).getOrElse("error while authenticating on the downstream service")))
}
