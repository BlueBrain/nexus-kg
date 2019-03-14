package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlClientError
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectCache, ProjectViewCoordinator, ViewCache}
import ch.epfl.bluebrain.nexus.kg.async.Caches._
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
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidResourceFormat, NoStatsForAggregateView, NotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
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

  def routes: Route = {
    val viewRefOpt = Some(viewRef)
    create(viewRef) ~ list(viewRefOpt) ~ sparql ~ elasticSearch ~ stats ~
      pathPrefix(IdSegment) { id =>
        concat(
          create(id, viewRef),
          update(id, viewRefOpt),
          tag(id, viewRefOpt),
          deprecate(id, viewRefOpt),
          fetch(id, viewRefOpt),
          tags(id, viewRefOpt)
        )
      }
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

  private def sparqlQuery(id: AbsoluteIri, view: View, query: String): Task[Either[Rejection, Json]] =
    indexers.sparql.copy(namespace = view.name).queryRaw(query).map[Either[Rejection, Json]](Right.apply).recoverWith {
      case SparqlClientError(_, body) => Task.pure(Left(InvalidResourceFormat(id.ref, body)))
    }

  private def sparql: Route =
    pathPrefix(IdSegment / "sparql") { id =>
      (post & pathEndOrSingleSlash & hasPermission(query)) {
        entity(as[String]) { query =>
          val result: Task[Either[Rejection, Json]] = viewCache.getBy[SparqlView](project.ref, id).flatMap {
            case Some(view) => sparqlQuery(id, view, query)
            case _          => Task.pure(Left(NotFound(id.ref)))
          }
          trace("searchSparql")(complete(result.runWithStatus(StatusCodes.OK)))
        }
      }
    }

  private def elasticSearch: Route =
    pathPrefix(IdSegment / "_search") { id =>
      (post & extract(_.request.uri.query()) & pathEndOrSingleSlash & hasPermission(query)) { params =>
        entity(as[Json]) { query =>
          val result: Task[Either[Rejection, Json]] = viewCache.getBy[View](project.ref, id).flatMap {
            case Some(v: ElasticSearchView) =>
              indexers.elasticSearch.searchRaw(query, Set(v.index), params).map(Right.apply)
            case Some(AggregateElasticSearchViewRefs(v)) =>
              allowedIndices(v).flatMap {
                case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
                case indices                    => indexers.elasticSearch.searchRaw(query, indices.toSet, params).map(Right.apply)
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

  private def allowedIndices(v: AggregateElasticSearchViewRefs): Task[List[String]] =
    v.value.toList.foldM(List.empty[String]) {
      case (acc, ViewRef(ref, id)) =>
        (cache.view.getBy[ElasticSearchView](ref, id) -> cache.project.getLabel(ref)).mapN {
          case (Some(view), Some(label)) if !view.deprecated && caller.hasPermission(acls, label, query) =>
            view.index :: acc
          case _ =>
            acc
        }
    }
}
