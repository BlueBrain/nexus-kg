package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.server.Directives.{handleExceptions, handleRejections, pathPrefix}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._

import scala.concurrent.Future

trait DefaultRouteHandling {

  protected def resourceRoutes(implicit caller: Caller): Route

  protected def searchRoutes(implicit caller: Caller): Route

  /**
    * Combining ''resourceRoutes'' with ''searchRoutes''
    * and add rejection and exception handling to it.
    *
    * @param initialPrefix the initial prefix to be consumed
    */
  def combinedRoutesFor(initialPrefix: String)(implicit iamClient: IamClient[Future]): Route =
    handleExceptions(ExceptionHandling.exceptionHandler) {
      handleRejections(RejectionHandling.rejectionHandler) {
        pathPrefix(initialPrefix) {
          extractCaller(iamClient) { implicit caller =>
            resourceRoutes ~ searchRoutes
          }
        }
      }
    }
}
