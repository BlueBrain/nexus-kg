package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.{ServiceDescription => AdminServiceDescription}
import ch.epfl.bluebrain.nexus.commons.es.client.{ServiceDescription => EsServiceDescription}
import ch.epfl.bluebrain.nexus.commons.sparql.client.{ServiceDescription => BlazegraphServiceDescription}
import ch.epfl.bluebrain.nexus.iam.client.types.{ServiceDescription => IamServiceDescription}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.Description
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes._
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
import ch.epfl.bluebrain.nexus.storage.client.types.{ServiceDescription => StorageServiceDescription}
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import journal.Logger
import kamon.instrumentation.akka.http.TracingDirectives.operationName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

/**
  * Akka HTTP route definition for service description and health status
  */
class AppInfoRoutes(serviceDescription: ServiceDescription, healthStatus: HealthStatusGroup)(
    implicit clients: Clients[Task]
) {

  def routes: Route =
    concat(
      (get & pathEndOrSingleSlash) {
        operationName("/") {
          complete(OK -> serviceDescription)
        }
      },
      (get & pathPrefix("health") & pathEndOrSingleSlash) {
        operationName("/health") {
          complete(healthStatus.check.runWithStatus(OK))
        }
      },
      (get & pathPrefix("version") & pathEndOrSingleSlash) {
        operationName("/version") {
          val serviceDescriptions = Task.sequence(
            List(
              Task.pure(serviceDescription),
              clients.admin.serviceDescription.map(identity).logError("admin"),
              clients.defaultRemoteStorage.serviceDescription.map(identity).logError("remoteStorage"),
              clients.iam.serviceDescription.map(identity).logError("iam"),
              clients.sparql.serviceDescription.map(identity).logError("blazegraph"),
              clients.elasticSearch.serviceDescription.map(identity).logError("elasticsearch")
            )
          )
          complete(serviceDescriptions.runWithStatus(OK))
        }
      }
    )
}

object AppInfoRoutes {

  private val logger = Logger[this.type]

  private def identity(value: AdminServiceDescription)      = ServiceDescription(value.name, value.version)
  private def identity(value: IamServiceDescription)        = ServiceDescription(value.name, value.version)
  private def identity(value: StorageServiceDescription)    = ServiceDescription(value.name, value.version)
  private def identity(value: EsServiceDescription)         = ServiceDescription(value.name, value.version)
  private def identity(value: BlazegraphServiceDescription) = ServiceDescription(value.name, value.version)

  private implicit class TaskErrorSyntax(private val task: Task[ServiceDescription]) extends AnyVal {
    def logError(serviceName: String): Task[ServiceDescription] =
      task.recover {
        case err =>
          logger.warn(s"Could not fetch service description for service '$serviceName'", err)
          ServiceDescription(serviceName, "unknown")
      }
  }

  final case class HealthStatusGroup(cassandra: CassandraHealthStatus, cluster: ClusterHealthStatus) {
    def check: Task[Health] = (cassandra.check, cluster.check).mapN(Health.apply)
  }

  /**
    * A collection of health status
    *
    * @param cassandra     the cassandra status
    * @param cluster       the cluster status
    */
  final case class Health(cassandra: Boolean, cluster: Boolean)

  object Health {
    implicit val endHealth: Encoder[Health] = {
      def status(value: Boolean): String = if (value) "up" else "inaccessible"
      Encoder.instance {
        case Health(cassandra, cluster) =>
          Json.obj("cassandra" -> status(cassandra).asJson, "cluster" -> status(cluster).asJson)
      }
    }
  }

  /**
    * A service description.
    *
    * @param name    the name of the service
    * @param version the current version of the service
    */
  final case class ServiceDescription(name: String, version: String)

  implicit val encoder: Encoder[List[ServiceDescription]] = Encoder.instance {
    _.foldLeft(Json.obj()) {
      case (acc, ServiceDescription(name, version)) => acc deepMerge Json.obj(name -> version.asJson)
    }
  }

  /**
    * Default factory method for building [[AppInfoRoutes]] instances.
    *
    * @param descConfig the description service configuration
    * @return a new [[AppInfoRoutes]] instance
    */
  def apply(
      descConfig: Description,
      healthStatus: HealthStatusGroup
  )(implicit clients: Clients[Task]): AppInfoRoutes =
    new AppInfoRoutes(ServiceDescription(descConfig.name, descConfig.version), healthStatus)

}
