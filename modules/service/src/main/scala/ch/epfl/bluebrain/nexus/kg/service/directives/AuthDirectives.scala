package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive0, Directive1}
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.UnexpectedUnsuccessfulHttpResponse
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait AuthDirectives {

  private val prefix = Path("kg")

  /**
    * Checks if the caller associated with ''cred'' has permissions ''perms'' on the path ''resource''.
    *
    * @param resource the path which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource(resource: Path, perm: Permission)(implicit iamClient: IamClient[Future],
                                                          cred: Option[OAuth2BearerToken],
                                                          ec: ExecutionContext): Directive0 =
    authorizeAsync {
      iamClient
        .getAcls(resource)
        .map(_.permissions.contains(perm))
        .recoverWith {
          case UnauthorizedAccess                      => Future.successful(false)
          case err: UnexpectedUnsuccessfulHttpResponse => Future.failed(err)
        }
    }

  /**
    * Checks if the caller associated with ''cred'' has permissions ''perms'' on the resource ''resource''.
    *
    * @param resource the resource id which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource[Id](resource: Id, perm: Permission)(implicit iamClient: IamClient[Future],
                                                            cred: Option[OAuth2BearerToken],
                                                            ec: ExecutionContext,
                                                            S: Show[Id]): Directive0 =
    authorizeResource(prefix ++ Path(resource.show), perm)

  /**
    * Authenticates the requested with the provided ''cred'' and returns the ''caller''
    *
    * @return the [[Caller]]
    */
  def authenticateCaller(implicit iamClient: IamClient[Future], cred: Option[OAuth2BearerToken]): Directive1[Caller] =
    onComplete(iamClient.getCaller(cred)).flatMap {
      case Success(caller) => provide(caller)
      case _               => reject(AuthorizationFailedRejection)
    }

}

object AuthDirectives extends AuthDirectives
