package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.javadsl.server.CustomRejection
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{extractCredentials, _}
import akka.http.scaladsl.server.directives.FutureDirectives.onComplete
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Identity}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.{Caller, IamClient}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.DownstreamServiceError

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object AuthDirectives {

  /**
    * Extracts the credentials from the HTTP Authorization Header and builds the [[AuthToken]]
    */
  def token: Directive1[Option[AuthToken]] =
    extractCredentials.flatMap {
      case Some(OAuth2BearerToken(value)) => provide(Some(AuthToken(value)))
      case Some(_)                        => reject(AuthorizationFailedRejection)
      case _                              => provide(None)
    }

  private def findIdentity(caller: Caller): Identity =
    caller.identities
      .find {
        case _: UserRef => true
        case _          => false
      }
      .getOrElse(Anonymous)

  /**
    * Authenticates the requested with the provided ''token'' and returns the ''caller''
    */
  def callerIdentity(implicit iamClient: IamClient[Future], token: Option[AuthToken]): Directive1[Identity] =
    onComplete(iamClient.getCaller(filterGroups = true)).flatMap {
      case Success(caller)             => provide(findIdentity(caller))
      case Failure(UnauthorizedAccess) => reject(AuthorizationFailedRejection)
      case Failure(err)                => reject(authorizationRejection(err))
    }

  /**
    * Signals that the authentication was rejected with an unexpected error.
    *
    * @param err the [[Rejection]]
    */
  final case class CustomAuthRejection(err: Rejection) extends CustomRejection

  private def authorizationRejection(err: Throwable) =
    CustomAuthRejection(
      DownstreamServiceError(Try(err.getMessage).getOrElse("error while authenticating on the downstream service")))

}
