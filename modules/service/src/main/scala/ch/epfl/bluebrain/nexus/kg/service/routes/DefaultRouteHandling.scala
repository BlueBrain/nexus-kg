package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{extractCredentials, handleExceptions, handleRejections, pathPrefix}
import akka.http.scaladsl.server.RouteConcatenation._
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Route}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris

abstract class DefaultRouteHandling()(implicit prefixes: PrefixUris) {

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  /**
    * Combining ''resourceRoutes'' with ''searchRoutes''
    * and add rejection and exception handling to it.
    *
    * @param initialPrefix the initial prefix to be consumed
    */
  def combinedRoutesFor(initialPrefix: String): Route =
    handleExceptions(ExceptionHandling.exceptionHandler(prefixes.ErrorContext)) {
      handleRejections(RejectionHandling.rejectionHandler(prefixes.ErrorContext)) {
        pathPrefix(initialPrefix) {
          extractCredentials {
            case Some(c: OAuth2BearerToken) => combine(Some(c))
            case Some(_)                    => reject(AuthorizationFailedRejection)
            case _                          => combine(None)
          }
        }
      }
    }

  private def combine(cred: Option[OAuth2BearerToken]) =
    writeRoutes(cred) ~ readRoutes(cred) ~ searchRoutes(cred)

}
