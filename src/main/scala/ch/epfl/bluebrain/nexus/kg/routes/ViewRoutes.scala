package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.InvalidOutputFormat
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection.{ElasticSearchProjection, SparqlProjection}
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.{Statistics, View}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound.notFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.routes.ViewRoutes._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.syntax._
import io.circe.{Encoder, Json}
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}") {
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
      // Retrieves view statistics
      (get & pathPrefix("statistics") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/statistics") {
          hasPermission(read).apply {
            val result = coordinator.statistics(id).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Retrieves progress for a view
      (get & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/progress") {
          hasPermission(read).apply {
            val result = coordinator.offset(id).map(_.map(ProgressWrapper.apply)).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Delete progress from a view. This triggers view restart with initial progress
      (delete & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/progress") {
          hasPermission(write).apply {
            val result = coordinator.restart(id).map(_.map(_ => ProgressWrapper.empty)).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Queries a view on the ElasticSearch endpoint
      (post & pathPrefix("_search") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/_search") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/sparql") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}") {
          (hasPermission(write) & projectNotDeprecated) {
            complete(views.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
          }
        }
      },
      // Fetch view
      (get & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/source") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/incoming") {
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
          operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/outgoing") {
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
      pathPrefix("projections") {
        concat(
          // Consume the projection id segment as underscore if present
          pathPrefix("_") {
            routesAllProjections(id)
          },
          // Consume the projection id segment
          pathPrefix(IdSegment) { projectionId =>
            routesProjection(id, projectionId)
          }
        )
      }
    )

  private def routesAllProjections(id: AbsoluteIri): Route =
    concat(
      // Queries the ElasticSearch endpoint of every projection on an aggregated search
      (post & pathPrefix("_search") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/_/_search") {
          (hasPermission(query) & extract(_.request.uri.query())) { params =>
            entity(as[Json]) { query =>
              val result =
                viewCache.getBy[CompositeView](project.ref, id).flatMap(runProjectionsSearch(params, id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Queries the Sparql endpoint of every projection on an aggregated search
      (post & pathPrefix("sparql") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/_/sparql") {
          hasPermission(query).apply {
            entity(as[String]) { query =>
              val result = viewCache.getBy[CompositeView](project.ref, id).flatMap(runProjectionsSearch(id, query))
              complete(result.runWithStatus(StatusCodes.OK))
            }
          }
        }
      },
      // Retrieves statistics from all projections
      (get & pathPrefix("statistics") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/_/statistics") {
          hasPermission(read).apply {
            val listed: Task[ListResults[Statistics]] =
              viewCache.getBy[CompositeView](project.ref, id).flatMap {
                case Some(view) => coordinator.projectionsStatistic(view)
                case None       => Task.pure(UnscoredQueryResults(0L, List.empty))
              }
            complete(listed.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Retrieves progress from all projections
      (get & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/_/progress") {
          hasPermission(read).apply {
            val listed: Task[ListResults[ProgressWrapper]] =
              viewCache.getBy[CompositeView](project.ref, id).flatMap {
                case Some(view) =>
                  coordinator.projectionsOffset(view).map(_.map(_.map(ProgressWrapper(_))))
                case None => Task.pure(UnscoredQueryResults(0L, List.empty))
              }
            complete(listed.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Delete progress from all projections. This triggers view restart with initial progress all projections
      (delete & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/_/progress") {
          hasPermission(write).apply {
            val result =
              coordinator.restartProjections(id).map(_.map(_ => ProgressWrapper.empty)).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      }
    )

  private def routesProjection(id: AbsoluteIri, projectionId: AbsoluteIri): Route =
    concat(
      // Queries a projection view on the ElasticSearch endpoint
      (post & pathPrefix("_search") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/{projectionId}/_search") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/{projectionId}/sparql") {
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
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/{projectionId}/statistics") {
          hasPermission(read).apply {
            val result = coordinator.statistics(id, projectionId).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Retrieves progress for a projection
      (get & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/{projectionId}/progress") {
          hasPermission(read).apply {
            val result =
              coordinator.offset(id, projectionId).map(_.map(ProgressWrapper.apply)).map(_.toRight(notFound(id.ref)))
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      },
      // Delete progress from a projection. This triggers view restart with initial progress for selected projection
      (delete & pathPrefix("progress") & pathEndOrSingleSlash) {
        operationName(s"/${config.http.prefix}/views/{org}/{project}/{id}/projections/{projectionId}/progress") {
          hasPermission(write).apply {
            val resultOption = coordinator.restart(id, projectionId).map(_.map(_ => ProgressWrapper.empty))
            val resultEither = resultOption.map(_.toRight(notFound(id.ref)))
            complete(resultEither.runWithStatus(StatusCodes.OK))
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

  private def runProjectionsSearch(
      id: AbsoluteIri,
      query: String
  ): Option[CompositeView] => Task[Either[Rejection, SparqlResults]] = {
    case Some(view) =>
      val projectionViews = view.projectionsBy[SparqlProjection].map(_.view)
      val resultListF     = Task.gatherUnordered(projectionViews.map(sparqlQuery(_, query)))
      resultListF.map(list => Right(list.foldLeft(SparqlResults.empty)(_ ++ _)))
    case _ =>
      Task.pure(Left(NotFound(id.ref)))
  }

  private def runProjectionsSearch(
      params: Uri.Query,
      id: AbsoluteIri,
      query: Json
  ): Option[CompositeView] => Task[Either[Rejection, Json]] = {
    case Some(view) => runSearchForIndices(params, query)(view.projectionsBy[ElasticSearchProjection].map(_.view.index))
    case _          => Task.pure(Left(NotFound(id.ref)))
  }

  private def runSearch(
      params: Uri.Query,
      id: AbsoluteIri,
      query: Json
  ): Option[View] => Task[Either[Rejection, Json]] = {
    case Some(v: ElasticSearchView)               => clients.elasticSearch.searchRaw(query, Set(v.index), params).map(Right.apply)
    case Some(agg: AggregateElasticSearchView[_]) => agg.queryableIndices.flatMap(runSearchForIndices(params, query))
    case _                                        => Task.pure(Left(NotFound(id.ref)))
  }

  private def runSearchForIndices(params: Uri.Query, query: Json)(indices: Set[String]): Task[Either[Rejection, Json]] =
    indices match {
      case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
      case indices                    => clients.elasticSearch.searchRaw(query, indices, params).map(Right.apply)
    }
}

object ViewRoutes {
  private[routes] final case class ProgressWrapper(offset: Offset)

  type ListResults[A] = QueryResults[IdentifiedValue[A]]

  private[routes] object ProgressWrapper {

    val empty: ProgressWrapper = ProgressWrapper(NoOffset)

    implicit val encoderProgress: Encoder[ProgressWrapper] =
      Encoder
        .instance[ProgressWrapper] {
          case ProgressWrapper(NoOffset) =>
            Json.obj("@type" -> nxv.NoProgress.prefix.asJson)
          case ProgressWrapper(Sequence(value)) =>
            Json.obj("@type" -> nxv.Sequential.prefix.asJson, nxv.offset.prefix -> value.asJson)
          case ProgressWrapper(tm: TimeBasedUUID) =>
            Json.obj("@type" -> nxv.TimeBased.prefix.asJson, nxv.offset.prefix -> tm.asInstant.asJson)
          case _ => Json.obj()
        }
        .mapJson(_.addContext(progressCtxUri))

  }

  private[routes] implicit val encoderListResultsOffset: Encoder[ListResults[ProgressWrapper]] = {
    implicit val progressEnc = ProgressWrapper.encoderProgress.mapJson(_.removeKeys("@context"))
    qrsEncoderLowPrio[IdentifiedValue[ProgressWrapper]].mapJson(_.addContext(progressCtxUri))
  }

  private[routes] implicit val encoderListResultsStatistics: Encoder[ListResults[Statistics]] = {
    implicit val statstisticsEnc = Statistics.statisticsEncoder.mapJson(_.removeKeys("@context"))
    qrsEncoderLowPrio[IdentifiedValue[Statistics]].mapJson(_.addContext(statisticsCtxUri))
  }

}
