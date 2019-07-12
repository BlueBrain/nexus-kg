package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.resources.Event.JsonLd._
import kamon.instrumentation.akka.http.TracingDirectives.operationName

class EventRoutes(acls: AccessControlLists, caller: Caller)(implicit as: ActorSystem, config: AppConfig)
    extends EventCommonRoutes {

  private val read: Permission                  = Permission.unsafe("resources/read")
  private implicit val acl: AccessControlLists  = acls
  private implicit val c: Caller                = caller
  private implicit val iamConf: IamClientConfig = config.iam.iamClient

  def routes(project: Project): Route = {
    lastEventId { offset =>
      operationName(s"/${config.http.prefix}/resources/{}/{}/events") {
        implicit val p: Project = project
        hasPermission(read).apply {
          complete(source(s"project=${project.uuid}", offset))
        }
      }
    }
  }

  def routes(org: Organization): Route =
    lastEventId { offset =>
      operationName(s"/${config.http.prefix}/resources/{}/events") {
        hasPermission(read, org.label).apply {
          complete(source(s"org=${org.uuid}", offset))
        }
      }
    }
}
