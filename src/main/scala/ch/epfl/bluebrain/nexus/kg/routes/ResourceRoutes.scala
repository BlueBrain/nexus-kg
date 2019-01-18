package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ResourceRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    cache: Caches[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "resources", acls, caller, cache.view) {

  import indexers._

  private implicit val viewCache = cache.view

  def routes: Route =
    list ~ pathPrefix(IdSegment) {
      case (`shaclSchemaUri`)    => new SchemaRoutes(resources, acls, caller).routes
      case (`viewSchemaUri`)     => new ViewRoutes(resources, acls, caller).routes
      case (`resolverSchemaUri`) => new ResolverRoutes(resources, acls, caller).routes
      case (`fileSchemaUri`)     => new FileRoutes(resources, acls, caller).routes
      case schema =>
        create(schema.ref) ~ list(schema.ref) ~
          pathPrefix(IdSegment) { id =>
            concat(
              update(id, Some(schema.ref)),
              create(id, schema.ref),
              tag(id, Some(schema.ref)),
              deprecate(id, Some(schema.ref)),
              fetch(id, Some(schema.ref))
            )
          }
    }

  def list: Route =
    (get & paginated & searchParams & hasPermission(resourceRead) & pathEndOrSingleSlash) { (pagination, params) =>
      trace(s"list$resourceName") {
        val defaultView = cache.view.getBy[ElasticView](project.ref, nxv.defaultElasticIndex.value)
        complete(defaultView.flatMap(v => resources.list(v, params, pagination)).runToFuture)
      }
    }
}
