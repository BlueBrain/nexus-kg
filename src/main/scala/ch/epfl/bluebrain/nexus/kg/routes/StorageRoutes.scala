package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.InvalidOutputFormat
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class StorageRoutes private[routes] (storages: Storages[Task], tags: Tags[Task])(implicit system: ActorSystem,
                                                                                 acls: AccessControlLists,
                                                                                 caller: Caller,
                                                                                 project: Project,
                                                                                 viewCache: ViewCache[Task],
                                                                                 indexers: Clients[Task],
                                                                                 config: AppConfig) {

  import indexers._

  /**
    * Routes for storages. Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/storages/{org}/{project}. E.g.: v1/storages/myorg/myproject </li>
    *   <li> {prefix}/resources/{org}/{project}/{storageSchemaUri}. E.g.: v1/resources/myorg/myproject/storage </li>
    * </ul>
    */
  def routes: Route =
    concat(
      // Create storage when id is not provided on the Uri (POST)
      (post & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          trace("createStorage") {
            complete(storages.create(project.base, source).value.runWithStatus(Created))
          }
        }
      },
      // List storages
      (get & paginated & searchParams(fixedSchema = storageSchemaUri) & pathEndOrSingleSlash & hasPermission(read)) {
        (pagination, params) =>
          trace(s"listStorage") {
            val listed = viewCache.getDefaultElasticSearch(project.ref).flatMap(storages.list(_, params, pagination))
            complete(listed.runWithStatus(OK))
          }
      },
      // Consume the storage id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      },
      new TagRoutes(tags, storageRef, write).routes
    )

  /**
    * Routes for storages when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/storages/{org}/{project}/{id}. E.g.: v1/storages/myorg/myproject/mystorage </li>
    *   <li> {prefix}/resources/{org}/{project}/{storageSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/storage/mystorage </li>
    * </ul>
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Create storage (PUT)
      (put & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          trace("createStorage") {
            complete(storages.create(Id(project.ref, id), source).value.runWithStatus(Created))
          }
        }
      },
      // Update storage
      (put & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        entity(as[Json]) { source =>
          trace("UpdateStorage") {
            complete(storages.update(Id(project.ref, id), rev, source).value.runWithStatus(OK))
          }
        }
      },
      // Deprecate storage
      (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        trace("deprecateStorage") {
          complete(storages.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
        }
      },
      // Fetch storage
      (get & outputFormat(strict = false, Compacted) & hasPermission(read) & pathEndOrSingleSlash) {
        case Binary => failWith(InvalidOutputFormat("Binary"))
        case format: NonBinaryOutputFormat =>
          trace(s"getStorage") {
            concat(
              (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                completeWithFormat(storages.fetch(Id(project.ref, id), rev).value.runWithStatus(OK))(format)
              },
              (parameter('tag) & noParameter('rev)) { tag =>
                completeWithFormat(storages.fetch(Id(project.ref, id), tag).value.runWithStatus(OK))(format)
              },
              (noParameter('tag) & noParameter('rev)) {
                completeWithFormat(storages.fetch(Id(project.ref, id)).value.runWithStatus(OK))(format)
              }
            )
          }
      }
    )
}
