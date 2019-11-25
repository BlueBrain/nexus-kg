package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.InvalidOutputFormat
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.{Statistics, View}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NoStatsForAggregateView, NotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import kamon.Kamon
import kamon.instrumentation.akka.http.TracingDirectives.operationName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (
    views: Views[Task],
    tags: Tags[Task],
    coordinator: ProjectViewCoordinator[Task]
)(
    implicit acls: AccessControlLists,
    caller: Caller,
    project: Project,
    projectCache: ProjectCache[Task],
    viewCache: ViewCache[Task],
    clients: Clients[Task],
    config: AppConfig,
    um: FromEntityUnmarshaller[String]
) {

  private val emptyEsList: Json = jsonContentOf("/elasticsearch/empty-list.json")

  import clients._

  /**
    * Routes for views. Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/views/{org}/{project}. E.g.: v1/views/myorg/myproject </li>
    *   <li> {prefix}/resources/{org}/{project}/{viewSchemaUri}. E.g.: v1/resources/myorg/myproject/view </li>
    * </ul>
    */
  def routes: Route =
    concat(
      // Create view when id is not provided on the Uri (POST)
      (post & noParameter('rev.as[Long]) & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}") {
          Kamon.currentSpan().tag("resource.operation", "create")
          (hasPermission(write) & projectNotDeprecated) {
            entity(as[Json]) { source =>
              complete(views.create(source).value.runWithStatus(Created))
            }
          }
        }
      },
      // List views
      (get & paginated & searchParams(fixedSchema = viewSchemaUri) & pathEndOrSingleSlash) { (page, params) =>
        operationName(s"/${config.http.prefix}/views/{}/{}") {
          extractUri { implicit uri =>
            hasPermission(read).apply {
              val listed = viewCache.getDefaultElasticSearch(project.ref).flatMap(views.list(_, params, page))
              complete(listed.runWithStatus(OK))
            }
          }
        }
      },
      // Consume the view id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      }
    )

  /**
    * Routes for views when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/views/{org}/{project}/{id}. E.g.: v1/views/myorg/myproject/myview </li>
    *   <li> {prefix}/resources/{org}/{project}/{viewSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/view/myview </li>
    * </ul>
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Fetches view statistics
      (get & pathPrefix("statistics") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/statistics") {
          hasPermission(read).apply {
            val result: Task[Either[Rejection, Statistics]] = viewCache.getBy[View](project.ref, id).flatMap {
              case Some(view: CompositeView) => coordinator.statistics(project, view.defaultSparqlView).map(Right(_))
              case Some(view: SingleView)    => coordinator.statistics(project, view).map(Right(_))
              case Some(_)                   => Task.pure(Left(NoStatsForAggregateView))
              case None                      => Task.pure(Left(NotFound(id.ref)))
            }
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Queries a view on the ElasticSearch endpoint
      (post & pathPrefix("_search") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/_search") {
          (hasPermission(query) & extract(_.request.uri.query())) { params =>
            entity(as[Json]) { query =>
              val result = viewCache.getBy[View](project.ref, id).flatMap(runSearch(params, id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Queries a view on the Sparql endpoint
      (post & pathPrefix("sparql") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/sparql") {
          hasPermission(query).apply {
            entity(as[String]) { query =>
              val result = viewCache.getBy[View](project.ref, id).flatMap(runSearch(id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Create or update a view (depending on rev query parameter)
      (put & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}") {
          (hasPermission(write) & projectNotDeprecated) {
            entity(as[Json]) { source =>
              parameter('rev.as[Long].?) {
                case None =>
                  Kamon.currentSpan().tag("resource.operation", "create")
                  complete(views.create(Id(project.ref, id), source).value.runWithStatus(Created))
                case Some(rev) =>
                  Kamon.currentSpan().tag("resource.operation", "update")
                  complete(views.update(Id(project.ref, id), rev, source).value.runWithStatus(OK))
              }
            }
          }
        }
      },
      // Deprecate view
      (delete & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
        operationName(s"/${config.http.prefix}/views/{}/{}/{}") {
          (hasPermission(write) & projectNotDeprecated) {
            complete(views.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
          }
        }
      },
      // Fetch view
      (get & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}") {
          outputFormat(strict = false, Compacted) {
            case format: NonBinaryOutputFormat =>
              hasPermission(read).apply {
                concat(
                  (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                    completeWithFormat(views.fetch(Id(project.ref, id), rev).value.runWithStatus(OK))(format)
                  },
                  (parameter('tag) & noParameter('rev)) { tag =>
                    completeWithFormat(views.fetch(Id(project.ref, id), tag).value.runWithStatus(OK))(format)
                  },
                  (noParameter('tag) & noParameter('rev)) {
                    completeWithFormat(views.fetch(Id(project.ref, id)).value.runWithStatus(OK))(format)
                  }
                )
              }
            case other => failWith(InvalidOutputFormat(other.toString))
          }
        }
      },
      // Fetch view source
      (get & pathPrefix("source") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/source") {
          hasPermission(read).apply {
            concat(
              (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
                complete(views.fetchSource(Id(project.ref, id), rev).value.runWithStatus(OK))
              },
              (parameter('tag) & noParameter('rev)) { tag =>
                complete(views.fetchSource(Id(project.ref, id), tag).value.runWithStatus(OK))
              },
              (noParameter('tag) & noParameter('rev)) {
                complete(views.fetchSource(Id(project.ref, id)).value.runWithStatus(OK))
              }
            )
          }
        }
      },
      // Incoming links
      (get & pathPrefix("incoming") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/incoming") {
          fromPaginated.apply { implicit page =>
            extractUri { implicit uri =>
              hasPermission(read).apply {
                val listed = viewCache.getDefaultSparql(project.ref).flatMap(views.listIncoming(id, _, page))
                complete(listed.runWithStatus(OK))
              }
            }
          }
        }
      },
      // Outgoing links
      (get & pathPrefix("outgoing") & parameter('includeExternalLinks.as[Boolean] ? true) & pathEndOrSingleSlash) {
        links =>
          operationName(s"/${config.http.prefix}/views/{}/{}/{}/outgoing") {
            fromPaginated.apply { implicit page =>
              extractUri { implicit uri =>
                hasPermission(read).apply {
                  val listed = viewCache.getDefaultSparql(project.ref).flatMap(views.listOutgoing(id, _, page, links))
                  complete(listed.runWithStatus(OK))
                }
              }
            }
          }
      },
      new TagRoutes("views", tags, viewRef, write).routes(id),
      // Consume the projection id segment
      pathPrefix("projections" / IdSegment) { projectionId =>
        routes(id, projectionId)
      }
    )

  private def routes(id: AbsoluteIri, projectionId: AbsoluteIri): Route =
    concat(
      // Queries a projection view on the ElasticSearch endpoint
      (post & pathPrefix("_search") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/projections/{}/_search") {
          (hasPermission(query) & extract(_.request.uri.query())) { params =>
            entity(as[Json]) { query =>
              val result =
                viewCache.getProjectionBy[View](project.ref, id, projectionId).flatMap(runSearch(params, id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Queries a projection view on the Sparql endpoint
      (post & pathPrefix("sparql") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/projections/{}/sparql") {
          hasPermission(query).apply {
            entity(as[String]) { query =>
              val result = viewCache.getProjectionBy[View](project.ref, id, projectionId).flatMap(runSearch(id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Retrieves statistics for a projection
      (get & pathPrefix("statistics") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{}/{}/{}/projections/{}/statistics") {
          hasPermission(read).apply {
            val result: Task[Either[Rejection, Statistics]] =
              viewCache.getProjectionBy[SingleView](project.ref, id, projectionId).flatMap {
                case Some(view) => coordinator.statistics(project, view).map(Right(_))
                case None       => Task.pure(Left(NotFound(id.ref)))
              }
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      }
    )

  private def sparqlQuery(view: SparqlView, query: String): Task[SparqlResults] =
    clients.sparql.copy(namespace = view.index).queryRaw(query)

  private def runSearch(id: AbsoluteIri, query: String): Option[View] => Task[Either[Rejection, SparqlResults]] = {
    case Some(v: SparqlView) => sparqlQuery(v, query).map(Right.apply)
    case Some(agg: AggregateSparqlView[_]) =>
      val resultListF = agg.queryableViews.flatMap { views =>
        Task.gatherUnordered(views.map(sparqlQuery(_, query)))
      }
      resultListF.map(list => Right(list.foldLeft(SparqlResults.empty)(_ ++ _)))
    case _ => Task.pure(Left(NotFound(id.ref)))
  }

  private def runSearch(
      params: Uri.Query,
      id: AbsoluteIri,
      query: Json
  ): Option[View] => Task[Either[Rejection, Json]] = {
    case Some(v: ElasticSearchView) => clients.elasticSearch.searchRaw(query, Set(v.index), params).map(Right.apply)
    case Some(agg: AggregateElasticSearchView[_]) =>
      agg.queryableIndices.flatMap {
        case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
        case indices                    => clients.elasticSearch.searchRaw(query, indices, params).map(Right.apply)
      }
    case _ => Task.pure(Left(NotFound(id.ref)))
  }
}
