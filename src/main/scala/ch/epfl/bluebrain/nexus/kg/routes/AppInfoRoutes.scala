package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.Description
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.{HealthStatusGroup, ServiceDescription}
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import io.circe.{Encoder, Printer}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

/**
  * Akka HTTP route definition for service description and health status
  */
class AppInfoRoutes(serviceDescription: ServiceDescription, healthStatus: HealthStatusGroup) {

  private implicit val printer = Printer.noSpaces.copy(dropNullValues = true)

  def routes: Route =
    (get & pathEndOrSingleSlash) {
      complete(serviceDescription)
    } ~ (pathPrefix("health") & get & pathEndOrSingleSlash) {
      complete(healthStatus.check.runToFuture)
    }

}

object AppInfoRoutes {

  /**
    * Enumeration type for possible status.
    */
  sealed trait Status extends Product with Serializable

  object Status {

    implicit val enc: Encoder[Status] = Encoder.encodeString.contramap {
      case Up           => "up"
      case Inaccessible => "inaccessible"
    }

    def apply(value: Boolean): Status =
      if (value) Up else Inaccessible

    /**
      * A service is up and running
      */
    final case object Up extends Status

    /**
      * A service is inaccessible from within the app
      */
    final case object Inaccessible extends Status

  }

  final case class HealthStatusGroup(cassandra: CassandraHealthStatus,
                                     cluster: ClusterHealthStatus,
                                     iam: IamHealthStatus,
                                     admin: AdminHealthStatus,
                                     elasticSearch: ElasticSearchHealthStatus,
                                     sparql: SparqlHealthStatus) {
    def check: Task[Health] =
      for {
        cassUp          <- cassandra.check
        clusterUp       <- cluster.check
        iamUp           <- iam.check
        adminUp         <- admin.check
        elasticSearchUp <- elasticSearch.check
        sparqlUp        <- sparql.check
      } yield
        Health(Status(cassUp),
               Status(clusterUp),
               Status(iamUp),
               Status(adminUp),
               Status(elasticSearchUp),
               Status(sparqlUp))
  }

  /**
    * A collection of health status
    *
    * @param cassandra     the cassandra status
    * @param cluster       the cluster status
    * @param iam           the IAM service status
    * @param admin         the ADMIN service status
    * @param elasticSearch the ElasticSearch indexer status
    * @param sparql        the SparQL indexer status
    */
  final case class Health(cassandra: Status,
                          cluster: Status,
                          iam: Status,
                          admin: Status,
                          elasticSearch: Status,
                          sparql: Status)

  /**
    * A service description.
    *
    * @param name    the name of the service
    * @param version the current version of the service
    */
  final case class ServiceDescription(name: String, version: String)

  /**
    * Default factory method for building [[AppInfoRoutes]] instances.
    *
    * @param descConfig the description service configuration
    * @return a new [[AppInfoRoutes]] instance
    */
  def apply(descConfig: Description, healthStatus: HealthStatusGroup): AppInfoRoutes =
    new AppInfoRoutes(ServiceDescription(descConfig.name, descConfig.version), healthStatus)

}
