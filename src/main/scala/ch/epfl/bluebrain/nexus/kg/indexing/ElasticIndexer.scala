package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.MonadError
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient.BulkOp
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticSearchIndexer._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexer which takes a resource event and calls ElasticSearch client with relevant update if required
  *
  * @param view      the view information describes how to index Documents
  * @param resources the resources operations
  */
class ElasticSearchIndexer[F[_]](view: ElasticSearchView, resources: Resources[F])(implicit config: AppConfig,
                                                                                   project: Project,
                                                                                   F: MonadError[F, Throwable]) {

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
  final def apply(ev: Event): F[Option[BulkOp]] =
    view.resourceTag.map(resources.fetch(ev.id, _, None)).getOrElse(resources.fetch(ev.id, None)).value.flatMap {
      case None                                       => F.raiseError(KgError.NotFound(ev.id.ref))
      case Some(resource) if validCandidate(resource) => F.pure(Some(transformAndIndex(resource)))
      case Some(_)                                    => F.pure(None)
    }

  private def validCandidate(resource: Resource): Boolean =
    view.resourceSchemas.isEmpty || view.resourceSchemas.contains(resource.schema.iri)

  private def transformAndIndex(res: Resource): BulkOp = {
    val primaryNode = IriNode(res.id.value)

    def asJson(g: Graph): Json = g.asJson(ctx, primaryNode).getOrElse(g.asJson)

    val transformed: Json = {
      val metaGraph = if (view.includeMetadata) Graph(res.metadata) else Graph()
      if (view.sourceAsText) asJson(metaGraph.add(primaryNode, nxv.originalSource, res.value.noSpaces))
      else res.value deepMerge asJson(metaGraph)
    }
    BulkOp.Index(view.index, config.elasticSearch.docType, res.id.value.asString, transformed.removeKeys("@context"))
  }

}

object ElasticSearchIndexer {

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  // $COVERAGE-OFF$
  final def start(view: ElasticSearchView, resources: Resources[Task], project: Project, restartOffset: Boolean)(
      implicit client: ElasticClient[Task],
      s: Scheduler,
      as: ActorSystem,
      config: AppConfig): ActorRef = {

    implicit val p = project

    val indexer = new ElasticSearchIndexer(view, resources)
    val init = () =>
      (for {
        _ <- view.createIndex[Task]
        _ <- if (view.rev > 1) client.deleteIndex(view.copy(rev = view.rev - 1).index) else Task.pure(true)
      } yield ()).runToFuture

    val index = (l: List[Event]) =>
      Task.sequence(l.removeDupIds.map(indexer(_))).flatMap(list => client.bulk(list.flatten)).runToFuture

    SequentialTagIndexer.start(
      IndexerConfig.builder
        .name(s"elasticSearch-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry(config.indexing.retry.maxCount, config.indexing.retry.strategy)
        .batch(config.indexing.batch, config.indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .index(index)
        .build)
  }
  // $COVERAGE-ON$

  private[indexing] val ctx: Json = jsonContentOf("/elasticsearch/default-context.json")
}
