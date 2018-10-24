package ch.epfl.bluebrain.nexus.kg.routes

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ViewRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    store: AttachmentStore[Task, AkkaIn, AkkaOut],
    config: AppConfig,
    um: FromEntityUnmarshaller[String])
    extends Schemed(resources, viewSchemaUri, "views", acls, caller) {

  import indexers._

  override def routes = super.routes ~ sparql ~ elasticSearch

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.view[Task]

  override def list: Route =
    (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) { deprecated =>
      trace("listViews") {
        complete(filterDeprecated(cache.views(wrapped.ref), deprecated).map(toQueryResults).runAsync)
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
        trace("searchSparql")(complete(result.runAsync))
    }
  private def elasticSearch: Route =
    (pathPrefix(IdSegment / "_search") & post & entity(as[Json]) & hasPermission(resourceRead)) { (id, query) =>
      (extract(_.request.uri.query()) & pathEndOrSingleSlash) { params =>
        val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
          views.find(_.id == id) match {
            case Some(v: ElasticView) => indexers.elastic.searchRaw(query, Set(v.index), params).map(Right.apply)
            case _                    => Task.pure(Left(NotFound(Ref(id))))
          }
        }
        trace("searchElastic")(complete(result.runAsync))
      }
    }

  override def transformCreate(j: Json): Json =
    transformView(j, UUID.randomUUID().toString.toLowerCase)

  override def transformUpdate(id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] = {
    val resId = Id(wrapped.ref, id)
    def fetchUuid(r: Resource): EitherT[Task, Rejection, String] =
      EitherT.fromEither(r.value.hcursor.get[String](nxv.uuid.prefix).left.map(_ => UnexpectedState(resId.ref)))
    val schemaOpt = Some(Latest(viewSchemaUri))
    resources.fetch(resId, schemaOpt).toRight(NotFound(resId.ref)).flatMap(fetchUuid).map(transformView(j, _))
  }

  private def transformView(source: Json, uuid: String): Json = {
    val transformed = source deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuid)).addContext(viewCtxUri)
    transformed.hcursor.get[Json]("mapping") match {
      case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
      case _                      => transformed
    }
  }
}