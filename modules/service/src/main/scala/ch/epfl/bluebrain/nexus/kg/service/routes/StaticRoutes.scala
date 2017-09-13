package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.routes.StaticRoutes.ServiceDescription
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import ch.epfl.bluebrain.nexus.kg.service.directives.PrefixDirectives._
import kamon.akka.http.KamonTraceDirectives.traceName

/**
  * Http route definitions that have constant outcomes per runtime.
  *
  * @param settings the service settings
  */
class StaticRoutes(settings: Settings) {

  private val desc = ServiceDescription(
    settings.Description.Name,
    settings.Description.Version,
    settings.Description.Environment)

  private def serviceDescriptionRoute = pathEndOrSingleSlash {
    get {
      traceName("serviceDescription") {
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
    * @param as an implicitly available actor system
    * @return a new ''StaticRoutes'' instance
    */
  final def apply()(implicit as: ActorSystem): StaticRoutes =
    new StaticRoutes(Settings(as))

  /**
    * Local data type that wraps service information.
    *
    * @param name    the name of the service
    * @param version the version of the service
    * @param env     the environment in which the service is run
    */
  final case class ServiceDescription(name: String, version: String, env: String)
}
