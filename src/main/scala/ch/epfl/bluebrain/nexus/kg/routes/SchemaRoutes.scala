package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.resources._
import monix.eval.Task

class SchemaRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    config: AppConfig)
    extends CommonRoutes(resources, "schemas", acls, caller, viewCache) {

  def routes: Route = {
    val shaclRefOpt = Some(shaclRef)
    create(shaclRef) ~ list(shaclRefOpt) ~
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
