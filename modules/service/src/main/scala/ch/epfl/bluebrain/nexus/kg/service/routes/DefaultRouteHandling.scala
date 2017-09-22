package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.server.Directives.{handleExceptions, handleRejections, pathPrefix}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._

trait DefaultRouteHandling {

  protected def resourceRoutes: Route

  protected def searchRoutes: Route

  /**
    * Combining ''resourceRoutes'' with ''searchRoutes''
    * and add rejection and exception handling to it.
    *
    * @param initialPrefix the initial prefix to be consumed
    */
  def combinedRoutesFor(initialPrefix: String): Route = handleExceptions(ExceptionHandling.exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix(initialPrefix) {
        resourceRoutes ~ searchRoutes
      }
    }
  }
}
