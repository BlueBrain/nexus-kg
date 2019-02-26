package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.Functor
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient.BulkOp
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticSearchIndexer._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, SequentialTagIndexer}
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

private class ElasticSearchIndexerMapping[F[_]: Functor](view: ElasticSearchView, resources: Resources[F])(
    implicit config: AppConfig,
    project: Project) {

  private val logger = Logger[this.type]

  /**
    * When an event is received, the current state is obtained and if the resource matches the view criteria a [[BulkOp]] is built.
    *
    * @param event event to be mapped to a Elastic Search insert query
    */
  final def apply(event: Event): F[Option[Identified[ProjectRef, BulkOp]]] =
    view.resourceTag.map(resources.fetch(event.id, _, None)).getOrElse(resources.fetch(event.id, None)).value.map {
      case Some(resource) if validCandidate(resource) => transformAndIndex(resource).map(event.id -> _)
      case _                                          => None
    }

  private def validCandidate(resource: Resource): Boolean =
    view.resourceSchemas.isEmpty || view.resourceSchemas.contains(resource.schema.iri)

  private def transformAndIndex(res: Resource): Option[BulkOp] = {
    val rootNode = IriNode(res.id.value)

    def asJson(g: Graph): DecoderResult[Json] = RootedGraph(rootNode, g).as[Json](ctx)

    val transformed: DecoderResult[Json] = {
      val metaGraph = if (view.includeMetadata) Graph(res.metadata) else Graph()
      if (view.sourceAsText) asJson(metaGraph.add(rootNode, nxv.originalSource, res.value.noSpaces))
      else asJson(metaGraph).map(metaJson => res.value deepMerge metaJson)
    }
    transformed match {
      case Left(err) =>
        logger.error(
          s"Could not convert resource with id '${res.id}' and value '${res.value}' from Graph back to json. Reason: '${err.message}'")
        None
      case Right(value) =>
        Some(
          BulkOp.Index(view.index, config.elasticSearch.docType, res.id.value.asString, value.removeKeys("@context")))
    }
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
      implicit client: ElasticSearchClient[Task],
      s: Scheduler,
      as: ActorSystem,
      config: AppConfig): ActorRef = {

    import ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
    implicit val p        = project
    implicit val indexing = config.indexing.elasticSearch

    val mapper = new ElasticSearchIndexerMapping(view, resources)
    val init =
      for {
        _ <- view.createIndex[Task]
        _ <- if (view.rev > 1) client.deleteIndex(view.copy(rev = view.rev - 1).index) else Task.pure(true)
      } yield ()

    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name(s"elasticSearch-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[ElasticSearchServerOrUnexpectedFailure](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .mapping(mapper.apply)
        .index(inserts => client.bulk(inserts.removeDupIds))
        .build)
  }
  // $COVERAGE-ON$

  private[indexing] val ctx: Json = jsonContentOf("/elasticsearch/default-context.json")
}
