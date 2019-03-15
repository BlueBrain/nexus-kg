package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectViewCoordinator}
import ch.epfl.bluebrain.nexus.kg.async.Caches._
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
    list(None) ~ pathPrefix(IdSegmentOrUnderscore) {
      case Underscore                    => new UnderscoreRoutes(resources, acls, caller, projectViewCoordinator).routes
      case SchemaId(`shaclSchemaUri`)    => new SchemaRoutes(resources, acls, caller).routes
      case SchemaId(`resolverSchemaUri`) => new ResolverRoutes(resources, acls, caller).routes
      case SchemaId(`fileSchemaUri`)     => new FileRoutes(resources, acls, caller).routes
      case SchemaId(`viewSchemaUri`)     => new ViewRoutes(resources, acls, caller, projectViewCoordinator).routes
      case SchemaId(`storageSchemaUri`)  => new StorageRoutes(resources, acls, caller).routes
      case SchemaId(schema) =>
        create(schema.ref) ~ list(Some(schema.ref)) ~
          pathPrefix(IdSegment) { id =>
            concat(
              create(id, schema.ref),
              update(id, Some(schema.ref)),
              tag(id, Some(schema.ref)),
              deprecate(id, Some(schema.ref)),
              fetch(id, Some(schema.ref)),
              tags(id, Some(schema.ref))
            )
          }
    } ~ create(unconstrainedRef)
}
