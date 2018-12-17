package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticViewRefs, ElasticView, SparqlView, ViewRef}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    config: AppConfig,
    um: FromEntityUnmarshaller[String])
    extends Schemed(resources, viewSchemaUri, "views", acls, caller) {

  private val emptyEsList: Json = jsonContentOf("/elastic/empty-list.json")

  import indexers._

  override def routes = super.routes ~ sparql ~ elasticSearch

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.view[Task](caller, acls)

  override def list: Route =
    (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) { deprecated =>
      trace("listViews") {
        val qr = filterDeprecated(cache.views(wrapped.ref), deprecated)
          .flatMap(_.flatTraverse(_.labeled.value.map(_.toList)))
          .map(toQueryResults)
        complete(qr.runToFuture)
      }
    }

  private def sparql: Route =
    (pathPrefix(IdSegment / "sparql") & post & entity(as[String]) & pathEndOrSingleSlash & hasPermission(resourceRead)) {
      (id, query) =>
        val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
          views.find(_.id == id) match {
            case Some(v: SparqlView) => indexers.sparql.copy(namespace = v.name).queryRaw(query).map(Right.apply)
            case _                   => Task.pure(Left(NotFound(Ref(id))))
          }
        }
        trace("searchSparql")(complete(result.runToFuture))
    }

  private def elasticSearch: Route =
    (pathPrefix(IdSegment / "_search") & post & entity(as[Json]) & hasPermission(resourceRead)) { (id, query) =>
      (extract(_.request.uri.query()) & pathEndOrSingleSlash) { params =>
        val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
          views.find(_.id == id) match {
            case Some(v: ElasticView) => indexers.elastic.searchRaw(query, Set(v.index), params).map(Right.apply)
            case Some(AggregateElasticViewRefs(v)) =>
              val aggIndicesF = v.value.foldLeft(Task.pure(Set.empty[String])) {
                case (accF, ViewRef(ref, id)) =>
                  for {
                    acc      <- accF
                    views    <- cache.views(ref).map(_.collect { case v: ElasticView if v.ref == ref && v.id == id => v })
                    labelOpt <- ref.toLabel(cache)
                  } yield
                    labelOpt match {
                      case Some(p) if caller.hasProjectPermission(acls, p, resourceRead) => acc ++ views.map(_.index)
                      case _                                                             => acc
                    }
              }
              aggIndicesF.flatMap {
                case indices if indices.isEmpty => Task.pure[Either[Rejection, Json]](Right(emptyEsList))
                case indices                    => indexers.elastic.searchRaw(query, indices, params).map(Right.apply)
              }

            case _ => Task.pure(Left(NotFound(Ref(id))))
          }
        }
        trace("searchElastic")(complete(result.runToFuture))
      }
    }

  override def transformGet(resource: ResourceV) =
    View(resource) match {
      case Right(r) =>
        val metadata = resource.metadata ++ resource.typeTriples
        val resValueF =
          r.labeled.getOrElse(r).map(r => resource.value.map(r, _.removeKeys("@context").addContext(viewCtxUri)))
        resValueF.map(v => resource.map(_ => v.copy(graph = v.graph ++ Graph(metadata))))
      case _ => Task.pure(resource)
    }
}
