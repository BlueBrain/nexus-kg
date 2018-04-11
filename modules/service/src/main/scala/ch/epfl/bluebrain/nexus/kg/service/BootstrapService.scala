package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.{ActorSystem, AddressFromURIString}
import akka.cluster.Cluster
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.service.routes._
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives.uriPrefix
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings

/**
  * Construct the service routes, operations and cluster.
  *
  * @param appConfig the application configuration
  * @param as        the implicitly available [[ActorSystem]]
  */
// $COVERAGE-OFF$
class BootstrapService(appConfig: AppConfig)(implicit as: ActorSystem) {

  private val baseUri = appConfig.http.publicUri

  private val static = uriPrefix(baseUri)(StaticRoutes(appConfig.description).routes)

  private val corsSettings = CorsSettings.defaultSettings
    .withAllowedMethods(List(GET, PUT, POST, DELETE, OPTIONS, HEAD))
    .withExposedHeaders(List(Location.name))

  val routes: Route = handleRejections(corsRejectionHandler) {
    cors(corsSettings)(static)
  }
  val cluster = Cluster(as)

  def joinCluster(): Unit = appConfig.cluster.seeds match {
    case None => cluster.join(cluster.selfAddress)
    case Some(seeds) =>
      val provided = seeds
        .split(",")
        .map(addr => AddressFromURIString(s"akka.tcp://${appConfig.description.actorSystemName}@$addr"))
        .toList
      cluster.joinSeedNodes(provided)
  }

  def leaveCluster(): Unit = cluster.leave(cluster.selfAddress)
}

object BootstrapService {

  /**
    * Constructs all the needed query settings for the service to start.
    *
    * @param appConfig the application configuration
    */
  final def apply(appConfig: AppConfig)(implicit as: ActorSystem): BootstrapService = new BootstrapService(appConfig)

}
// $COVERAGE-ON$
