package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ResourceRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "resources", acls, caller) {

  import indexers._

  def routes: Route =
    list ~ pathPrefix(IdSegment) {
      case (`shaclSchemaUri`)    => new SchemaRoutes(resources, acls, caller).routes
      case (`viewSchemaUri`)     => new ViewRoutes(resources, acls, caller).routes
      case (`resolverSchemaUri`) => new ResolverRoutes(resources, acls, caller).routes
      case (`fileSchemaUri`)     => new FileRoutes(resources, acls, caller).routes
      case schema =>
        val schemaRef = Ref(schema)
        create(schemaRef) ~ list(schemaRef) ~
          pathPrefix(IdSegment) { id =>
            concat(
              update(id, Some(schemaRef)),
              create(id, schemaRef),
              tag(id, Some(schemaRef)),
              deprecate(id, Some(schemaRef)),
              fetch(id, Some(schemaRef))
            )
          }
    }

  def list: Route =
    (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
      (deprecated, pagination) =>
        trace(s"list$resourceName") {
          complete(cache.views(projectRef).flatMap(v => resources.list(v, deprecated, pagination)).runToFuture)
        }
    }
}
