package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.Description
import ch.epfl.bluebrain.nexus.kg.routes.ServiceDescriptionRoutes.ServiceDescription
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Printer
import io.circe.generic.auto._

/**
  * Akka HTTP route definition for service description
  */
class ServiceDescriptionRoutes(serviceDescription: ServiceDescription) {

  private implicit val printer = Printer.noSpaces.copy(dropNullValues = true)

  def routes: Route = (get & pathEndOrSingleSlash) {
    complete(serviceDescription)
  }

}

object ServiceDescriptionRoutes {

  /**
    * A service description.
    *
    * @param name    the name of the service
    * @param version the current version of the service
    */
  final case class ServiceDescription(name: String, version: String)

  /**
    * Default factory method for building [[ServiceDescriptionRoutes]] instances.
    *
    * @param descConfig the description service configuration
    * @return a new [[ServiceDescriptionRoutes]] instance
    */
  def apply(descConfig: Description): ServiceDescriptionRoutes =
    new ServiceDescriptionRoutes(ServiceDescription(descConfig.name, descConfig.version))

}
