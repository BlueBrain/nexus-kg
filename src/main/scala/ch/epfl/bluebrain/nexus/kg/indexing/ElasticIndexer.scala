package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder
import java.util.regex.Pattern.quote

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{ElasticConfig, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexer._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexer which takes a resource event and calls ElasticSearch client with relevant update if required
  *
  * @param client    the ElasticSearch client
  * @param index     the ElasticSearch index
  * @param resources the resources operations
  */
class ElasticIndexer[F[_]](client: ElasticClient[F], index: String, resources: Resources[F])(
    implicit config: ElasticConfig,
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
    resources.fetch(ev.id, None).value.flatMap {
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

  private def indexResource(res: Resource): F[Unit] = {
    val primaryNode = Some(IriNode(res.id.value))
    val graph       = res.metadata(_.iri) ++ res.typeGraph

    val payload = graph.asJson(resourceCtx, primaryNode).getOrElse(graph.asJson)
    val merged  = payload deepMerge Json.obj("_source" -> Json.fromString(res.value.noSpaces))
    client.update(index, config.docType, res.id.elasticId, merged)
  }

}

object ElasticIndexer {

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view      the view for which to start the index
    * @param resources the resources operations
    */
  final def start(view: View, resources: Resources[Task])(implicit as: ActorSystem,
                                                          s: Scheduler,
                                                          config: ElasticConfig,
                                                          persistence: PersistenceConfig): ActorRef = {

    implicit val mt         = ActorMaterializer()
    implicit val ul         = HttpClient.taskHttpClient
    implicit val jsonClient = HttpClient.withTaskUnmarshaller[Json]

    val mapping = jsonContentOf("/elastic/mapping.json", Map(quote("{{docType}}") -> config.docType))
    val client  = ElasticClient[Task](config.base)
    val index   = elasticIndex(view)
    val indexer = new ElasticIndexer(client, index, resources)
    SequentialTagIndexer.startLocal[Event](
      () => client.createIndexIfNotExist(index, mapping).map(_ => ()).runAsync,
      (ev: Event) => indexer(ev).runAsync,
      persistence.queryJournalPlugin,
      tag = s"project=${view.ref.id}",
      name = s"elastic-indexer-${view.name}"
    )
  }

  def elasticIndex(view: View)(implicit config: ElasticConfig): String =
    s"${config.indexPrefix}_${view.name}"

  private[indexing] implicit class ResIdSyntax(id: ResId) {
    def elasticId: String = URLEncoder.encode(id.value.show, "UTF-8").toLowerCase
  }
}
