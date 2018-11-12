package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.MonadError
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient.BulkOp
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexer._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
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
class ElasticIndexer[F[_]](view: ElasticView, resources: Resources[F])(implicit config: AppConfig,
                                                                       labeledProject: LabeledProject,
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
      case None                                       => F.raiseError(NotFound(ev.id.ref))
      case Some(resource) if validCandidate(resource) => F.pure(Some(transformAndIndex(resource)))
      case Some(_)                                    => F.pure(None)
    }

  private def validCandidate(resource: Resource): Boolean =
    view.resourceSchemas.isEmpty || view.resourceSchemas.contains(resource.schema.iri)

  private def transformAndIndex(res: Resource): BulkOp = {
    val primaryNode = IriNode(res.id.value)

    def asJson(g: Graph): Json = g.asJson(ctx, primaryNode).getOrElse(g.asJson)

    val transformed: Json = {
      val metaGraph = if (view.includeMetadata) Graph(res.metadata ++ res.typeTriples) else Graph()
      if (view.sourceAsText) asJson(metaGraph.add(primaryNode, nxv.originalSource, res.value.noSpaces))
      else res.value deepMerge asJson(metaGraph)
    }
    BulkOp.Index(view.index, config.elastic.docType, res.id.value.asString, transformed.removeKeys("@context"))
  }

}

object ElasticIndexer {

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view           the view for which to start the index
    * @param resources      the resources operations
    * @param labeledProject project to which the resource belongs containing label information (account label and project label)
    */
  // $COVERAGE-OFF$
  final def start(view: ElasticView, resources: Resources[Task], labeledProject: LabeledProject)(
      implicit client: ElasticClient[Task],
      s: Scheduler,
      as: ActorSystem,
      config: AppConfig): ActorRef = {

    implicit val lb = labeledProject

    val indexer = new ElasticIndexer(view, resources)
    val init = () =>
      (for {
        _ <- view.createIndex[Task]
        _ <- if (view.rev > 1) client.deleteIndex(view.copy(rev = view.rev - 1).index) else Task.pure(true)
      } yield ()).runAsync

    val index = (l: List[Event]) =>
      Task.sequence(l.removeDupIds.map(indexer(_))).flatMap(list => client.bulk(list.flatten)).runAsync

    SequentialTagIndexer.start(
      IndexerConfig.builder
        .name(s"elastic-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry(config.indexing.retry.maxCount, config.indexing.retry.strategy)
        .batch(config.indexing.batch, config.indexing.batchTimeout)
        .init(init)
        .index(index)
        .build)
  }
  // $COVERAGE-ON$

  private[indexing] val ctx: Json = jsonContentOf("/elastic/default-context.json")
}
