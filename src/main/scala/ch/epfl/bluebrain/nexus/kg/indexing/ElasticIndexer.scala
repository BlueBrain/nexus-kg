package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{ElasticConfig, IamConfig, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexer._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexer which takes a resource event and calls ElasticSearch client with relevant update if required
  *
  * @param client    the ElasticSearch client
  * @param view      the view information describes how to index Documents
  * @param resources the resources operations
  */
class ElasticIndexer[F[_]](client: ElasticClient[F], view: ElasticView, resources: Resources[F])(
    implicit config: ElasticConfig,
    iamConfig: IamConfig,
    F: MonadError[F, Throwable],
    ucl: HttpClient[F, Json]) {
  private val revKey = "_rev"
  private val index  = elasticIndex(view)

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
      case Some(resource) if validCandidate(resource) =>
        fetchRevision(ev.id) flatMap {
          case Some(rev) if resource.rev > rev => transformAndIndex(resource)
          case None                            => transformAndIndex(resource)
          case _                               => F.pure(())
        }
      case Some(_) => F.pure(())
    }
  }

  private def validCandidate(resource: Resource): Boolean = {
    val validSchema = view.resourceSchemas.isEmpty || view.resourceSchemas.contains(resource.schema.iri)
    view.resourceTag match {
      case Some(tag) if resource.tags.contains(tag) => resource.tags(tag) == resource.rev && validSchema
      case Some(_)                                  => false
      case None                                     => validSchema
    }
  }

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client
      .get[Json](index, config.docType, id.elasticId, include = Set(revKey))
      .map(_.hcursor.get[Long](revKey).toOption)
      .handleError {
        case ElasticClientError(StatusCodes.NotFound, _) => None
      }

  private def transformAndIndex(res: Resource): F[Unit] = {
    val primaryNode = IriNode(res.id.value)

    def transform: F[Json] = {
      val metaGraph = if (view.includeMetadata) res.metadata ++ res.typeGraph else Graph()
      val graph: F[Graph] =
        if (view.sourceAsBlob)
          F.pure(metaGraph.add(primaryNode, nxv.originalSource, res.value.noSpaces))
        else
          resources.materialize(res).value.flatMap {
            case Left(r)  => F.raiseError(r)
            case Right(r) => F.pure(metaGraph ++ r.value.graph)
          }
      graph.map(g => g.asJson(ctx, Some(primaryNode)).getOrElse(g.asJson).removeKeys("@context"))
    }

    transform.flatMap(json => client.create(index, config.docType, res.id.elasticId, json))
  }

}

object ElasticIndexer {

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view      the view for which to start the index
    * @param resources the resources operations
    */
  // $COVERAGE-OFF$
  final def start(view: ElasticView, resources: Resources[Task])(implicit as: ActorSystem,
                                                                 s: Scheduler,
                                                                 config: ElasticConfig,
                                                                 iamConfig: IamConfig,
                                                                 persistence: PersistenceConfig): ActorRef = {
    implicit val mt         = ActorMaterializer()
    implicit val ul         = HttpClient.taskHttpClient
    implicit val jsonClient = HttpClient.withTaskUnmarshaller[Json]

    val client  = ElasticClient[Task](config.base)
    val indexer = new ElasticIndexer(client, view, resources)
    SequentialTagIndexer.startLocal[Event](
      () => client.createIndexIfNotExist(elasticIndex(view), view.mapping).map(_ => ()).runAsync,
      (ev: Event) => indexer(ev).runAsync,
      persistence.queryJournalPlugin,
      tag = s"project=${view.ref.id}",
      name = s"elastic-indexer-${view.name}"
    )
  }
  // $COVERAGE-ON$

  def elasticIndex(view: View)(implicit config: ElasticConfig): String =
    s"${config.indexPrefix}_${view.name}"

  private[indexing] implicit class ResIdSyntax(id: ResId) {
    def elasticId: String = URLEncoder.encode(id.value.show, "UTF-8").toLowerCase
  }
  private[indexing] val ctx: Json =
    resourceCtx appendContextOf Json.obj(
      "@context" -> Json.obj("_original_source" -> Json.fromString(nxv.originalSource.show)))

}
