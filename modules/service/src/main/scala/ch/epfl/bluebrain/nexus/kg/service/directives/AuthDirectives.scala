package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{authorizeAsync, extractCredentials, provide}
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
    * Checks if the caller has permissions ''perms'' on the path ''resource''.
    *
    * @param resource the path which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource(resource: Path, perm: Permission)(implicit iamClient: IamClient[Future],
                                                          caller: Caller,
                                                          ec: ExecutionContext): Directive0 =

    authorizeAsync {
      iamClient
        .getAcls(resource)
        .map(_.toMap.values.find(!_.contains(perm)).isEmpty)
        .recoverWith {
          case UnauthorizedAccess                      => Future.successful(false)
          case err: UnexpectedUnsuccessfulHttpResponse => Future.failed(err)
        }

  }

  /**
    * Checks if the caller has permissions ''perms'' on the resource ''resource''.
    *
    * @param resource the resource id which the caller wants to access
    * @param perm     the permission to be checked
    */
  def authorizeResource[Id](resource: Id, perm: Permission)(implicit iamClient: IamClient[Future],
                                                            caller: Caller,
                                                            ec: ExecutionContext,
                                                            S: Show[Id]): Directive0 =
    authorizeResource(prefix ++ Path(resource.show), perm)

  /**
    * Extracts the caller using the provided [[IamClient]]
    *
    * @param iam the iamClient which makes the necessary requests to fetch the caller
    */
  def extractCaller(iam: IamClient[Future]): Directive1[Caller] = {

    def inner(cred: Option[OAuth2BearerToken]): Directive1[Caller] =
      onComplete(iam.getCaller(cred)).flatMap {
        case Success(caller) => provide(caller)
        case _               => reject(AuthorizationFailedRejection)
      }

    extractCredentials.flatMap {
      case Some(cred @ OAuth2BearerToken(_)) => inner(Some(cred))
      case Some(_)                           => reject(AuthorizationFailedRejection)
      case _                                 => inner(None)
    }
  }

}

object AuthDirectives extends AuthDirectives
