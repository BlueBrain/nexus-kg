package ch.epfl.bluebrain.nexus.kg.routes

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
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ResolverRoutes private[routes] (resolvers: Resolvers[Task], tags: Tags[Task])(implicit acls: AccessControlLists,
                                                                                    caller: Caller,
                                                                                    project: Project,
                                                                                    viewCache: ViewCache[Task],
                                                                                    indexers: Clients[Task],
                                                                                    config: AppConfig) {

  import indexers._

  /**
    * Routes for resolvers. Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/resolvers/{org}/{project}. E.g.: v1/views/myorg/myproject </li>
    *   <li> {prefix}/resources/{org}/{project}/{resolverSchemaUri}. E.g.: v1/resources/myorg/myproject/resolver </li>
    * </ul>
    */
  def routes: Route =
    concat(
      // Create resolver when id is not provided on the Uri (POST)
      (post & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          trace("createResolver") {
            complete(resolvers.create(source).value.runWithStatus(Created))
          }
        }
      },
      // List resolvers
      (get & paginated & searchParams(fixedSchema = resolverSchemaUri) & pathEndOrSingleSlash & hasPermission(read) & extractUri) {
        (pagination, params, uri) =>
          trace("listResolver") {
            implicit val u = uri
            val listed     = viewCache.getDefaultElasticSearch(project.ref).flatMap(resolvers.list(_, params, pagination))
            complete(listed.runWithStatus(OK))
          }
      },
      // Consume the '_' segment
      pathPrefix("_") {
        routesResourceResolution
      },
      // Consume the resolver id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      }
    )

  /**
    * Routes for resource resolution.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/resolvers/{org}/{project}/_. E.g.: v1/resolvers/myorg/myproject/_ </li>
    * </ul>
    */
  def routesResourceResolution: Route =
    // Consume the resource id segment
    (pathPrefix(IdSegment) & get & outputFormat(strict = false, Compacted) & hasPermission(read) & pathEndOrSingleSlash) {
      case (_, Binary) => failWith(InvalidOutputFormat("Binary"))
      case (id, format: NonBinaryOutputFormat) =>
        trace("resolveResource") {
          concat(
            (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
              completeWithFormat(resolvers.resolve(id, rev).value.runWithStatus(OK))(format)
            },
            (parameter('tag) & noParameter('rev)) { tag =>
              completeWithFormat(resolvers.resolve(id, tag).value.runWithStatus(OK))(format)
            },
            (noParameter('tag) & noParameter('rev)) {
              completeWithFormat(resolvers.resolve(id).value.runWithStatus(OK))(format)
            }
          )
        }
    }

  /**
    * Routes for resource resolution using the provided resolver.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/resolvers/{org}/{project}/{id}. E.g.: v1/resolvers/myorg/myproject/myresolver </li>
    *   <li> {prefix}/resources/{org}/{project}/{resolverSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/resolver/myresolver </li>
    *   <li> {prefix}/resources/{org}/{project}/_/{id}. E.g.: v1/resources/myorg/myproject/_/myresolver </li>
    * </ul>
    */
  def routesResourceResolution(id: AbsoluteIri): Route = {
    val resolverId = Id(project.ref, id)
    // Consume the resource id segment
    (pathPrefix(IdSegment) & get & outputFormat(strict = false, Compacted) & hasPermission(read) & pathEndOrSingleSlash) {
      case (_, Binary) => failWith(InvalidOutputFormat("Binary"))
      case (resourceId, format: NonBinaryOutputFormat) =>
        trace("resolveResource") {
          concat(
            (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
              completeWithFormat(resolvers.resolve(resolverId, resourceId, rev).value.runWithStatus(OK))(format)
            },
            (parameter('tag) & noParameter('rev)) { tag =>
              completeWithFormat(resolvers.resolve(resolverId, resourceId, tag).value.runWithStatus(OK))(format)
            },
            (noParameter('tag) & noParameter('rev)) {
              completeWithFormat(resolvers.resolve(resolverId, resourceId).value.runWithStatus(OK))(format)
            }
          )
        }
    }
  }

  /**
    * Routes for resolvers when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/resolvers/{org}/{project}/{id}. E.g.: v1/resolvers/myorg/myproject/myresolver </li>
    *   <li> {prefix}/resources/{org}/{project}/_/{id}. E.g.: v1/resources/myorg/myproject/_/myresolver </li>
    *   <li> {prefix}/resources/{org}/{project}/{resolverSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/resolver/myresolver </li>
    * </ul>
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Create or update a resolver (depending on rev query parameter)
      (put & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
        entity(as[Json]) { source =>
          parameter('rev.as[Long].?) {
            case None =>
              trace("createResolver") {
                complete(resolvers.create(Id(project.ref, id), source).value.runWithStatus(Created))
              }
            case Some(rev) =>
              trace("updateResolver") {
                complete(resolvers.update(Id(project.ref, id), rev, source).value.runWithStatus(OK))
              }
          }
        }
      },
      // Deprecate resolver
      (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        trace("deprecateResolver") {
          complete(resolvers.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
        }
      },
      // Fetch resolver
      (get & outputFormat(strict = false, Compacted) & hasPermission(read) & pathEndOrSingleSlash) {
        case Binary => failWith(InvalidOutputFormat("Binary"))
        case format: NonBinaryOutputFormat =>
          trace("getResolver") {
            concat(
              (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                completeWithFormat(resolvers.fetch(Id(project.ref, id), rev).value.runWithStatus(OK))(format)
              },
              (parameter('tag) & noParameter('rev)) { tag =>
                completeWithFormat(resolvers.fetch(Id(project.ref, id), tag).value.runWithStatus(OK))(format)
              },
              (noParameter('tag) & noParameter('rev)) {
                completeWithFormat(resolvers.fetch(Id(project.ref, id)).value.runWithStatus(OK))(format)
              }
            )
          }
      },
      // Incoming links
      (pathPrefix("incoming") & get & fromPaginated & pathEndOrSingleSlash & hasPermission(read)) { pagination =>
        trace("incomingLinksResolver") {
          val listed = viewCache.getDefaultSparql(project.ref).flatMap(resolvers.listIncoming(id, _, pagination))
          complete(listed.map[RejOrLinkResults](Right.apply).runWithStatus(OK))
        }
      },
      // Outgoing links
      (pathPrefix("outgoing") & get & fromPaginated & parameter('includeExternalLinks.as[Boolean] ? true) & pathEndOrSingleSlash &
        hasPermission(read)) { (pagination, links) =>
        trace("outgoingLinksResolver") {
          val listed = viewCache.getDefaultSparql(project.ref).flatMap(resolvers.listOutgoing(id, _, pagination, links))
          complete(listed.map[RejOrLinkResults](Right.apply).runWithStatus(OK))
        }
      },
      new TagRoutes(tags, resolverRef, write).routes(id),
      routesResourceResolution(id)
    )
}
