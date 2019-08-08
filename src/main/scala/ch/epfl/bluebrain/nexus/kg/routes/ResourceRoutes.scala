package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.InvalidOutputFormat
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import kamon.Kamon
import kamon.instrumentation.akka.http.TracingDirectives.operationName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ResourceRoutes private[routes] (resources: Resources[Task], tags: Tags[Task], schema: Ref)(
    implicit acls: AccessControlLists,
    caller: Caller,
    project: Project,
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    config: AppConfig
) {

  import indexers._

  /**
    * Routes for resources. Those routes should get triggered after the following segments have been consumed:
    * {prefix}/resources/{org}/{project}/{schema}. E.g.: v1/resources/myorg/myproject/myschema
    */
  def routes: Route =
    concat(
      // Create resource when id is not provided on the Uri (POST)
      (post & noParameter('rev.as[Long]) & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}") {
          Kamon.currentSpan().tag("resource.operation", "create")
          (hasPermission(write) & projectNotDeprecated) {
            entity(as[Json]) { source =>
              complete(resources.create(schema, source).value.runWithStatus(Created))
            }
          }
        }
      },
      // List resources
      (get & paginated & searchParams(fixedSchema = schema.iri) & pathEndOrSingleSlash) { (page, params) =>
        extractUri { implicit uri =>
          operationName(s"/${config.http.prefix}/resources/{}/{}/{}") {
            hasPermission(read).apply {
              val listed = viewCache.getDefaultElasticSearch(project.ref).flatMap(resources.list(_, params, page))
              complete(listed.runWithStatus(OK))
            }
          }
        }
      },
      // Consume the resource id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      }
    )

  /**
    * Routes for resources when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * {prefix}/resources/{org}/{project}/{schema}/{id}. E.g.: v1/resources/myorg/myproject/myschema/myresource
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Create or update a resource (depending on rev query parameter)
      (put & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}") {
          (hasPermission(write) & projectNotDeprecated) {
            entity(as[Json]) { source =>
              parameter('rev.as[Long].?) {
                case None =>
                  Kamon.currentSpan().tag("resource.operation", "create")
                  complete(resources.create(Id(project.ref, id), schema, source).value.runWithStatus(Created))
                case Some(rev) =>
                  Kamon.currentSpan().tag("resource.operation", "update")
                  complete(resources.update(Id(project.ref, id), rev, schema, source).value.runWithStatus(OK))
              }
            }
          }
        }
      },
      // Deprecate resource
      (delete & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}") {
          (hasPermission(write) & projectNotDeprecated) {
            complete(resources.deprecate(Id(project.ref, id), rev, schema).value.runWithStatus(OK))
          }
        }
      },
      // Fetch resource
      (get & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}") {
          outputFormat(strict = false, Compacted) {
            case Binary => failWith(InvalidOutputFormat("Binary"))
            case format: NonBinaryOutputFormat =>
              hasPermission(read).apply {
                concat(
                  (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                    completeWithFormat(resources.fetch(Id(project.ref, id), rev, schema).value.runWithStatus(OK))(
                      format
                    )
                  },
                  (parameter('tag) & noParameter('rev)) { tag =>
                    completeWithFormat(resources.fetch(Id(project.ref, id), tag, schema).value.runWithStatus(OK))(
                      format
                    )
                  },
                  (noParameter('tag) & noParameter('rev)) {
                    completeWithFormat(resources.fetch(Id(project.ref, id), schema).value.runWithStatus(OK))(format)
                  }
                )
              }
          }
        }
      },
      // Fetch resource source
      (get & pathPrefix("source") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}/source") {
          hasPermission(read).apply {
            concat(
              (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                complete(resources.fetchSource(Id(project.ref, id), rev, schema).value.runWithStatus(OK))
              },
              (parameter('tag) & noParameter('rev)) { tag =>
                complete(resources.fetchSource(Id(project.ref, id), tag, schema).value.runWithStatus(OK))
              },
              (noParameter('tag) & noParameter('rev)) {
                complete(resources.fetchSource(Id(project.ref, id), schema).value.runWithStatus(OK))
              }
            )
          }
        }
      },
      // Incoming links
      (get & pathPrefix("incoming") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}/incoming") {
          fromPaginated.apply { implicit page =>
            extractUri { implicit uri =>
              hasPermission(read).apply {
                val listed = viewCache.getDefaultSparql(project.ref).flatMap(resources.listIncoming(id, _, page))
                complete(listed.runWithStatus(OK))
              }
            }
          }
        }
      },
      // Outgoing links
      (get & pathPrefix("outgoing") & parameter('includeExternalLinks.as[Boolean] ? true) & pathEndOrSingleSlash) {
        links =>
          operationName(s"/${config.http.prefix}/resources/{}/{}/{}/{}/outgoing") {
            fromPaginated.apply { implicit page =>
              extractUri { implicit uri =>
                hasPermission(read).apply {
                  val listed =
                    viewCache.getDefaultSparql(project.ref).flatMap(resources.listOutgoing(id, _, page, links))
                  complete(listed.runWithStatus(OK))
                }
              }
            }
          }
      },
      new TagRoutes("resources", tags, schema, write).routes(id)
    )
}

object ResourceRoutes {
  val write: Permission = Permission.unsafe("resources/write")

}
