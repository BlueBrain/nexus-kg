package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure
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
import ch.epfl.bluebrain.nexus.kg.indexing.{View, ViewStatistics}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NoStatsForAggregateView, NotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Json
import kamon.Kamon
import kamon.instrumentation.akka.http.TracingDirectives.operationName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (views: Views[Task],
                                  tags: Tags[Task],
                                  projectViewCoordinator: ProjectViewCoordinator[Task])(
    implicit acls: AccessControlLists,
    caller: Caller,
    project: Project,
    projectCache: ProjectCache[Task],
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    config: AppConfig,
    um: FromEntityUnmarshaller[String]) {

  private val emptyEsList: Json = jsonContentOf("/elasticsearch/empty-list.json")

  import indexers._

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
      sparqlRoute,
      elasticSearch,
      stats,
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
            case Binary => failWith(InvalidOutputFormat("Binary"))
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
      new TagRoutes("views", tags, viewRef, write).routes(id)
    )

  private def sparqlQuery(view: SparqlView, query: String): Task[SparqlResults] = {
    import ch.epfl.bluebrain.nexus.kg.instances.sparqlErrorMonadError
    implicit val retryer: Retry[Task, SparqlServerOrUnexpectedFailure] = Retry(config.sparql.query.retryStrategy)
    indexers.sparql.copy(namespace = view.index).queryRaw(query).retry
  }

  private def sparqlRoute: Route =
    (post & pathPrefix(IdSegment / "sparql") & pathEndOrSingleSlash) { id =>
      operationName(s"/${config.http.prefix}/views/{}/{}/{}/sparql") {
        hasPermission(query).apply {
          entity(as[String]) { query =>
            val result: Task[Either[Rejection, SparqlResults]] = viewCache.getBy[View](project.ref, id).flatMap {
              case Some(v: SparqlView) => sparqlQuery(v, query).map(Right.apply)
              case Some(agg: AggregateSparqlView[_]) =>
                val resultListF = agg.queryableViews.flatMap { views =>
                  Task.gatherUnordered(views.map(sparqlQuery(_, query)))
                }
                resultListF.map(list => Right(list.foldLeft(SparqlResults.empty)(_ ++ _)))
              case _ => Task.pure(Left(NotFound(id.ref)))
            }
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      }
    }

  private def elasticSearch: Route =
    (post & pathPrefix(IdSegment / "_search") & pathEndOrSingleSlash) { id =>
      operationName(s"/${config.http.prefix}/views/{}/{}/{}/_search") {
        (hasPermission(query) & extract(_.request.uri.query())) { params =>
          entity(as[Json]) { query =>
            import ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
            implicit val retryer: Retry[Task, ElasticSearchServerOrUnexpectedFailure] =
              Retry(config.elasticSearch.query.retryStrategy)

            val result: Task[Either[Rejection, Json]] = viewCache.getBy[View](project.ref, id).flatMap {
              case Some(v: ElasticSearchView) =>
                indexers.elasticSearch.searchRaw(query, Set(v.index), params).map(Right.apply).retry
              case Some(agg: AggregateElasticSearchView[_]) =>
                agg.queryableIndices.flatMap {
                  case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
                  case indices                    => indexers.elasticSearch.searchRaw(query, indices, params).map(Right.apply).retry
                }
              case _ => Task.pure(Left(NotFound(id.ref)))
            }
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      }
    }

  private def stats: Route =
    (get & pathPrefix(IdSegment / "statistics") & pathEndOrSingleSlash) { id =>
      operationName(s"/${config.http.prefix}/views/{}/{}/{}/statistics") {
        hasPermission(read).apply {
          val result: Task[Either[Rejection, ViewStatistics]] = viewCache.getBy[View](project.ref, id).flatMap {
            case Some(view: SingleView) => projectViewCoordinator.viewStatistics(project, view).map(Right(_))
            case Some(_)                => Task.pure(Left(NoStatsForAggregateView))
            case None                   => Task.pure(Left(NotFound(id.ref)))
          }
          complete(result.runWithStatus(StatusCodes.OK))
        }
      }
    }
}
