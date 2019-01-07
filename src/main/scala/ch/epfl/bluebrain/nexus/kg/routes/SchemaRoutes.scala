package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.resources._
import monix.eval.Task

class SchemaRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    config: AppConfig)
    extends CommonRoutes(resources, "schemas", acls, caller) {

  def routes: Route = {
    val shaclRefOpt = Option(shaclRef)
    create(shaclRef) ~ list(shaclRef) ~
      pathPrefix(IdSegment) { id =>
        concat(
          update(id, shaclRefOpt),
          create(id, shaclRef),
          tag(id, shaclRefOpt),
          deprecate(id, shaclRefOpt),
          fetch(id, shaclRefOpt)
        )
      }
  }
}
