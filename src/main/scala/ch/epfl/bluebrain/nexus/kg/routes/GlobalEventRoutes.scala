package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.persistence.TaggingAdapter
import ch.epfl.bluebrain.nexus.kg.resources.Event.JsonLd._

class GlobalEventRoutes(acls: AccessControlLists, caller: Caller)(implicit as: ActorSystem, config: AppConfig)
    extends EventCommonRoutes {

  private val read: Permission                  = Permission.unsafe("events/read")
  private implicit val acl: AccessControlLists  = acls
  private implicit val c: Caller                = caller
  private implicit val iamConf: IamClientConfig = config.iam.iamClient

  def routes: Route =
    (lastEventId & hasPermissionOnRoot(read)) { offset =>
      complete(source(TaggingAdapter.EventTag, offset))
    }
}
