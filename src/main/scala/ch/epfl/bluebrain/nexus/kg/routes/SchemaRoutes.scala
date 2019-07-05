package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.InvalidOutputFormat
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.routes.SchemaRoutes._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import kamon.instrumentation.akka.http.TracingDirectives.operationName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class SchemaRoutes private[routes] (schemas: Schemas[Task], tags: Tags[Task])(implicit acls: AccessControlLists,
                                                                              caller: Caller,
                                                                              project: Project,
                                                                              viewCache: ViewCache[Task],
                                                                              indexers: Clients[Task],
                                                                              config: AppConfig) {

  import indexers._

  /**
    * Routes for schemas. Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/schemas/{org}/{project}. E.g.: v1/schemas/myorg/myproject </li>
    *   <li> {prefix}/resources/{org}/{project}/{shaclSchemaUri}. E.g.: v1/resources/myorg/myproject/schema </li>
    * </ul>
    */
  def routes: Route =
    concat(
      // Create schema when id is not provided on the Uri (POST)
      (post & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          operationName("createSchema") {
            complete(schemas.create(source).value.runWithStatus(Created))
          }
        }
      },
      // List schemas
      (get & paginated & searchParams(fixedSchema = shaclSchemaUri) & pathEndOrSingleSlash & hasPermission(read)) {
        (page, params) =>
          extractUri { implicit uri =>
            operationName("listSchema") {
              val listed = viewCache.getDefaultElasticSearch(project.ref).flatMap(schemas.list(_, params, page))
              complete(listed.runWithStatus(OK))
            }
          }
      },
      // Consume the schema id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      }
    )

  /**
    * Routes for schemas when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/schemas/{org}/{project}/{id}. E.g.: v1/schemas/myorg/myproject/myschema </li>
    *   <li> {prefix}/resources/{org}/{project}/{shaclSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/schema/myschema </li>
    * </ul>
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Create or update a schema (depending on rev query parameter)
      (put & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          parameter('rev.as[Long].?) {
            case None =>
              operationName("createSchema") {
                complete(schemas.create(Id(project.ref, id), source).value.runWithStatus(Created))
              }
            case Some(rev) =>
              operationName("updateSchema") {
                complete(schemas.update(Id(project.ref, id), rev, source).value.runWithStatus(OK))
              }
          }
        }
      },
      // Deprecate schema
      (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        operationName("deprecateSchema") {
          complete(schemas.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
        }
      },
      // Fetch schema
      (get & outputFormat(strict = false, Compacted) & hasPermission(read) & pathEndOrSingleSlash) {
        case Binary => failWith(InvalidOutputFormat("Binary"))
        case format: NonBinaryOutputFormat =>
          operationName("getSchema") {
            concat(
              (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                completeWithFormat(schemas.fetch(Id(project.ref, id), rev).value.runWithStatus(OK))(format)
              },
              (parameter('tag) & noParameter('rev)) { tag =>
                completeWithFormat(schemas.fetch(Id(project.ref, id), tag).value.runWithStatus(OK))(format)
              },
              (noParameter('tag) & noParameter('rev)) {
                completeWithFormat(schemas.fetch(Id(project.ref, id)).value.runWithStatus(OK))(format)
              }
            )
          }
      },
      // Incoming links
      (pathPrefix("incoming") & get & pathEndOrSingleSlash & hasPermission(read)) {
        fromPaginated.apply { implicit page =>
          extractUri { implicit uri =>
            operationName("incomingLinksSchema") {
              val listed = viewCache.getDefaultSparql(project.ref).flatMap(schemas.listIncoming(id, _, page))
              complete(listed.runWithStatus(OK))
            }
          }
        }
      },
      // Outgoing links
      (pathPrefix("outgoing") & get & parameter('includeExternalLinks.as[Boolean] ? true) & pathEndOrSingleSlash & hasPermission(
        read)) { links =>
        fromPaginated.apply { implicit page =>
          extractUri { implicit uri =>
            operationName("outgoingLinksSchema") {
              val listed = viewCache.getDefaultSparql(project.ref).flatMap(schemas.listOutgoing(id, _, page, links))
              complete(listed.runWithStatus(OK))
            }
          }
        }
      },
      new TagRoutes(tags, shaclRef, write).routes(id)
    )
}

object SchemaRoutes {
  val write: Permission = Permission.unsafe("schemas/write")
}
