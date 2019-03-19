package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.async.Caches._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectCache, ProjectViewCoordinator, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.{View, ViewStatistics}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NoStatsForAggregateView, NotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (resources: Resources[Task],
                                  acls: AccessControlLists,
                                  caller: Caller,
                                  projectViewCoordinator: ProjectViewCoordinator[Task])(
    implicit project: Project,
    cache: Caches[Task],
    indexers: Clients[Task],
    config: AppConfig,
    um: FromEntityUnmarshaller[String])
    extends CommonRoutes(resources, "views", acls, caller) {

  private val emptyEsList: Json                          = jsonContentOf("/elasticsearch/empty-list.json")
  private val transformation: Transformation[Task, View] = Transformation.view

  private implicit val projectCache: ProjectCache[Task]    = cache.project
  private implicit val viewCache: ViewCache[Task]          = cache.view
  private implicit val esClient: ElasticSearchClient[Task] = indexers.elasticSearch
  private implicit val ujClient: HttpClient[Task, Json]    = indexers.uclJson

  def routes: Route =
    create(viewRef) ~ list(viewRef) ~ sparql ~ elasticSearch ~ stats ~
      pathPrefix(IdSegment) { id =>
        concat(
          create(id, viewRef),
          update(id, viewRef),
          tag(id, viewRef),
          deprecate(id, viewRef),
          fetch(id, viewRef),
          tags(id, viewRef)
        )
      }

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.view[Task](caller, acls)

  override def transform(r: ResourceV): Task[ResourceV] = transformation(r)

  override def transform(payload: Json) = {
    val transformed = payload.addContext(viewCtxUri) deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuid()))
    transformed.hcursor.get[Json]("mapping") match {
      case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
      case _                      => transformed
    }
  }

  private def sparqlQuery(view: SparqlView, query: String): Task[SparqlResults] = {
    import ch.epfl.bluebrain.nexus.kg.instances.sparqlErrorMonadError
    implicit val retryer: Retry[Task, SparqlServerOrUnexpectedFailure] = Retry(config.sparql.query.retryStrategy)
    indexers.sparql.copy(namespace = view.name).queryRaw(query).retry
  }

  private def sparql: Route =
    pathPrefix(IdSegment / "sparql") { id =>
      (post & pathEndOrSingleSlash & hasPermission(query)) {
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
          trace("searchSparql") {
            complete(result.runWithStatus(StatusCodes.OK))
          }
        }
      }
    }

  private def elasticSearch: Route =
    pathPrefix(IdSegment / "_search") { id =>
      (post & extract(_.request.uri.query()) & pathEndOrSingleSlash & hasPermission(query)) { params =>
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
          trace("searchElasticSearch")(complete(result.runWithStatus(StatusCodes.OK)))
        }
      }
    }

  private def stats: Route =
    pathPrefix(IdSegment / "statistics") { id =>
      (get & hasPermission(read)) {
        val result: Task[Either[Rejection, ViewStatistics]] = viewCache.getBy[View](project.ref, id).flatMap {
          case Some(view: SingleView) => projectViewCoordinator.viewStatistics(project, view).map(Right(_))
          case Some(_)                => Task.pure(Left(NoStatsForAggregateView))
          case None                   => Task.pure(Left(NotFound(id.ref)))
        }
        complete(result.runWithStatus(StatusCodes.OK))
      }
    }
}
