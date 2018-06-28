package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexer._
import ch.epfl.bluebrain.nexus.kg.resolve.{InProjectResolution, Resolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexer which takes a resource event and calls ElasticSearch client with relevant update if required
  *
  * @param client the ElasticSearch client
  */
class ElasticIndexer[F[_]: Resolution](client: ElasticClient[F], index: String)(implicit repo: Repo[F],
                                                                                config: ElasticConfig,
                                                                                F: MonadError[F, Throwable],
                                                                                ucl: HttpClient[F, Json]) {
  private val revKey = "_rev"

  /**
    * When an event is received, the current state is obtained.
    * Afterwards, the current revision is fetched from the ElasticSearch index.
    * If the current revision is not found or it is smaller than the state's revision, the state gets indexed.
    * Otherwise the event it is skipped.
    *
    * @param ev event to index
    * @return Unit wrapped in the context F.
    *         This method will raise errors if something goes wrong
    */
  final def apply(ev: Event): F[Unit] = {
    fetch(ev.id, None).value.flatMap {
      case None => F.raiseError(NotFound(ev.id.ref))
      case Some(resource) =>
        fetchRevision(ev.id) flatMap {
          case Some(rev) if resource.rev > rev => indexResource(resource)
          case None                            => indexResource(resource)
          case _                               => F.pure(())
        }
    }
  }

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client
      .get[Json](index, config.docType, id.elasticId, include = Set(revKey))
      .map(j => j.hcursor.get[Long](revKey).toOption)

  private def indexResource(res: Resource): F[Unit] =
    materialize(res).value.flatMap {
      case Left(err) =>
        F.raiseError(err)
      case Right(r) =>
        val payload = metadataJson(r) deepMerge Json.obj("original" -> Json.fromString(json(r).noSpaces))
        client.update(index, config.docType, r.id.elasticId, payload)
    }

  private def metadataJson(res: ResourceV): Json =
    json(res.id.value, res.metadata(_.iri) ++ res.typeGraph, res.value.ctx).removeKeys("@context")

  private def json(res: ResourceV): Json =
    json(res.id.value, res.value.graph, res.value.ctx).removeKeys("@context")

  private def json(id: AbsoluteIri, graph: Graph, ctx: Json): Json = {
    val primaryNode     = Some(IriNode(id))
    val mergedCtx: Json = ctx mergeContext resourceCtx
    graph.asJson(mergedCtx, primaryNode).getOrElse(graph.asJson)
  }

}

object ElasticIndexer {

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view     the view for which to start the index
    * @param pluginId the persistence query plugin id to query the event log
    */
  final def start(
      view: View,
      pluginId: String
  )(implicit repo: Repo[Task], as: ActorSystem, s: Scheduler, config: ElasticConfig): ActorRef = {

    implicit val mt         = ActorMaterializer()
    implicit val ul         = HttpClient.taskHttpClient
    implicit val jsonClient = HttpClient.withTaskUnmarshaller[Json]
    implicit val res        = InProjectResolution[Task](view.ref)

    val client  = ElasticClient[Task](config.base, ElasticQueryClient[Task](config.base))
    val indexer = new ElasticIndexer(client, elasticIndex(view))
    SequentialTagIndexer.startLocal[Event](
      (ev: Event) => indexer(ev).runAsync,
      pluginId,
      tag = s"project=${view.ref.id}",
      name = s"elastic-indexer-${view.name}"
    )
  }

  private def elasticIndex(view: View)(implicit config: ElasticConfig): String =
    s"${config.indexPrefix}_${view.name}"

  private[indexing] implicit class ResIdSyntax(id: ResId) {

    def elasticId: String =
      URLEncoder.encode(id.value.show, "UTF-8").toLowerCase
  }
}
