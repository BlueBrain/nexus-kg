package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{
  handleExceptions,
  handleRejections,
  pathPrefix,
  extractCredentials,
  complete
}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._

trait DefaultRouteHandling {

  protected def resourceRoutes(credentials: OAuth2BearerToken): Route

  protected def searchRoutes(credentials: OAuth2BearerToken): Route

  /**
    * Combining ''resourceRoutes'' with ''searchRoutes''
    * and add rejection and exception handling to it.
    *
    * @param initialPrefix the initial prefix to be consumed
    */
  def combinedRoutesFor(initialPrefix: String): Route = handleExceptions(ExceptionHandling.exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix(initialPrefix) {
        extractCredentials {
          case Some(cred: OAuth2BearerToken) => resourceRoutes(cred) ~ searchRoutes(cred)
          case _                             => complete(StatusCodes.Unauthorized)
        }
      }
    }
  }
}
