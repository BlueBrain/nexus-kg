package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    config: AppConfig,
    um: FromEntityUnmarshaller[String])
    extends CommonRoutes(resources, "views", acls, caller) {

  private val emptyEsList: Json                          = jsonContentOf("/elastic/empty-list.json")
  private val transformation: Transformation[Task, View] = Transformation.view

  import indexers._

  def routes: Route = {
    val viewRefOpt = Option(viewRef)
    create(viewRef) ~ list(viewRef) ~ sparql ~ elasticSearch ~
      pathPrefix(IdSegment) { id =>
        concat(
          update(id, viewRefOpt),
          create(id, viewRef),
          tag(id, viewRefOpt),
          deprecate(id, viewRefOpt),
          fetch(id, viewRefOpt)
        )
      }
  }

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.view[Task](caller, acls)
  override def transform(r: ResourceV)                         = transformation(r)

  override def list(schema: Ref): Route =
    (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) { deprecated =>
      trace("listViews") {
        val qr = filterDeprecated(cache.views(projectRef), deprecated)
          .flatMap(_.flatTraverse(_.labeled.value.map(_.toList)))
          .map(toQueryResults)
        complete(qr.runToFuture)
      }
    }

  private def sparql: Route =
    pathPrefix(IdSegment / "sparql") { id =>
      (post & entity(as[String]) & hasPermission(resourceRead) & pathEndOrSingleSlash) { query =>
        val result: Task[Either[Rejection, Json]] = cache.views(projectRef).flatMap { views =>
          views.find(_.id == id) match {
            case Some(v: SparqlView) => indexers.sparql.copy(namespace = v.name).queryRaw(query).map(Right.apply)
            case _                   => Task.pure(Left(NotFound(Ref(id))))
          }
        }
        trace("searchSparql")(complete(result.runToFuture))
      }
    }

  private def elasticSearch: Route =
    pathPrefix(IdSegment / "_search") { id =>
      (post & entity(as[Json]) & extract(_.request.uri.query()) & hasPermission(resourceRead) & pathEndOrSingleSlash) {
        (query, params) =>
          val result: Task[Either[Rejection, Json]] = cache.views(projectRef).flatMap { views =>
            views.find(_.id == id) match {
              case Some(v: ElasticView) => indexers.elastic.searchRaw(query, Set(v.index), params).map(Right.apply)
              case Some(v: AggregateElasticView[_]) =>
                v.indices.flatMap {
                  case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
                  case indices                    => indexers.elastic.searchRaw(query, indices, params).map(Right.apply)
                }
              case _ => Task.pure(Left(NotFound(Ref(id))))
            }
          }
          trace("searchElastic")(complete(result.runToFuture))
      }
    }
}
