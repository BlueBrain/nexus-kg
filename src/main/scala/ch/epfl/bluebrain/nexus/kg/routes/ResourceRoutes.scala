package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator
import ch.epfl.bluebrain.nexus.kg.cache.Caches._
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import monix.eval.Task

class ResourceRoutes private[routes] (resources: Resources[Task],
                                      acls: AccessControlLists,
                                      caller: Caller,
                                      projectViewCoordinator: ProjectViewCoordinator[Task])(implicit as: ActorSystem,
                                                                                            project: Project,
                                                                                            cache: Caches[Task],
                                                                                            indexers: Clients[Task],
                                                                                            config: AppConfig)
    extends CommonRoutes(resources, "resources", acls, caller) {

  private implicit val viewCache = cache.view

  def routes: Route =
    events ~ list ~ pathPrefix(IdSegmentOrUnderscore) {
      case Underscore                    => new UnderscoreRoutes(resources, acls, caller, projectViewCoordinator).routes
      case SchemaId(`shaclSchemaUri`)    => new SchemaRoutes(resources, acls, caller).routes
      case SchemaId(`resolverSchemaUri`) => new ResolverRoutes(resources, acls, caller).routes
      case SchemaId(`fileSchemaUri`)     => new FileRoutes(resources, acls, caller).routes
      case SchemaId(`viewSchemaUri`)     => new ViewRoutes(resources, acls, caller, projectViewCoordinator).routes
      case SchemaId(`storageSchemaUri`)  => new StorageRoutes(resources, acls, caller).routes
      case SchemaId(schema) =>
        create(schema.ref) ~ list(schema.ref) ~
          pathPrefix(IdSegment) { id =>
            concat(
              create(id, schema.ref),
              update(id, schema.ref),
              tag(id, schema.ref),
              deprecate(id, schema.ref),
              fetch(id, schema.ref),
              tags(id, schema.ref)
            )
          }
    } ~ create(unconstrainedRef)

  private def events: Route =
    (pathPrefix("events") & pathEndOrSingleSlash) {
      new EventRoutes(acls, caller).routes
    }
}
