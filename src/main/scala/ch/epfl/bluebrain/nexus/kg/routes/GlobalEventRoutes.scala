package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import akka.persistence.query._
import akka.persistence.query.scaladsl.EventsByTagQuery
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.kg.resources.Event.JsonLd._
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.persistence.TaggingAdapter

class GlobalEventRoutes(acls: AccessControlLists, caller: Caller)(implicit as: ActorSystem, config: AppConfig)
    extends EventCommonRoutes {

  override val pq: EventsByTagQuery =
    PersistenceQuery(as).readJournalFor[EventsByTagQuery](config.persistence.queryJournalPlugin)

  private val read: Set[Permission]             = Set(Permission.unsafe("resources/read"), Permission.unsafe("events/read"))
  private implicit val acl: AccessControlLists  = acls
  private implicit val c: Caller                = caller
  private implicit val iamConf: IamClientConfig = config.iam.iamClient

  def routes: Route =
    (lastEventId & hasPermissionsOnRoot(read)) { offset =>
      complete(source(TaggingAdapter.EventTag, offset))
    }
}
