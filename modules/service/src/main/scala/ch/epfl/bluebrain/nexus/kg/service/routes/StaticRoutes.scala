package ch.epfl.bluebrain.nexus.kg.service.routes

import kamon.akka.http.KamonTraceDirectives.operationName
import akka.http.scaladsl.model.StatusCodes
import ch.epfl.bluebrain.nexus.kg.service.routes.StaticRoutes.ServiceDescription
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives._
import akka.http.scaladsl.server.Directives._
import io.circe.generic.auto._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.DescriptionConfig
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

/**
  * Http route definitions that have constant outcomes per runtime.
  *
  * @param description the application description
  */
class StaticRoutes(description: DescriptionConfig) {

  private val desc =
    ServiceDescription(description.name, description.version)

  private def serviceDescriptionRoute = pathEndOrSingleSlash {
    get {
      operationName("serviceDescription") {
        complete(StatusCodes.OK -> desc)
      }
    }
  }

  private def docsRoute =
    pathPrefix("docs") {
      pathEndOrSingleSlash {
        extractUri { uri =>
          redirect(uri.copy(path = stripTrailingSlashes(uri.path) / "kg" / "index.html"), StatusCodes.MovedPermanently)
        }
      } ~
        pathPrefix("kg") {
          pathEndOrSingleSlash {
            redirectToTrailingSlashIfMissing(StatusCodes.MovedPermanently) {
              getFromResource("docs/index.html")
            }
          } ~
            getFromResourceDirectory("docs")
        }
    }

  def routes: Route = serviceDescriptionRoute ~ docsRoute
}

object StaticRoutes {

  /**
    * Constructs a new ''StaticRoutes'' instance that defines the static http routes of the service.
    *
    * @param description the application description
    * @return a new ''StaticRoutes'' instance
    */
  final def apply(description: DescriptionConfig): StaticRoutes = new StaticRoutes(description)

  /**
    * Local data type that wraps service information.
    *
    * @param name    the name of the service
    * @param version the version of the service
    */
  final case class ServiceDescription(name: String, version: String)
}
