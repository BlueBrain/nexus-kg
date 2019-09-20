package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem, Props}
import cats.Functor
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
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
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import ch.epfl.bluebrain.nexus.sourcing.projections._
import io.circe.Json
import journal.Logger
import kamon.Kamon
import monix.execution.atomic.AtomicLong
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF._

private class ElasticSearchIndexerMapping[F[_]: Functor](view: ElasticSearchView, resources: Resources[F])(
    implicit config: AppConfig,
    project: Project
) {

  private val logger          = Logger[this.type]
  private val metaKeys        = metaPredicates.map(_.prefix).toSeq
  private val metadataOptions = MetadataOptions(linksAsIri = false, expandedLinks = true)

  /**
    * When an event is received, the current state is obtained and if the resource matches the view criteria a [[BulkOp]] is built.
    *
    * @param event event to be mapped to a Elastic Search insert query
    */
  final def apply(event: Event): F[Option[Identified[ProjectRef, BulkOp]]] =
    view.resourceTag
      .filter(_.trim.nonEmpty)
      .map(resources.fetch(event.id, _, metadataOptions, None))
      .getOrElse(resources.fetch(event.id, metadataOptions, None))
      .value
      .map {
        case Right(res) if validSchema(view, res) && validTypes(view, res) => deleteOrIndexTransformed(res)
        case Right(res) if validSchema(view, res)                          => Some(delete(res))
        case _                                                             => None
      }

  private def deleteOrIndexTransformed(res: ResourceV): Option[Identified[ProjectRef, BulkOp]] =
    if (res.deprecated && !view.includeDeprecated) Some(delete(res))
    else indexTransformed(res).map(res.id -> _)

  private def delete(res: ResourceV): Identified[ProjectRef, BulkOp] =
    res.id -> BulkOp.Delete(view.index, res.id.value.asString)

  private def indexTransformed(res: ResourceV): Option[BulkOp] = {
    val rootNode = IriNode(res.id.value)

    def asJson(g: Graph): DecoderResult[Json] = RootedGraph(rootNode, g).as[Json](ctx)

    val transformed: DecoderResult[Json] = {
      val metaGraph    = if (view.includeMetadata) Graph(res.metadata(metadataOptions)) else Graph()
      val keysToRemove = if (view.includeMetadata) Seq.empty[String] else metaKeys
      if (view.sourceAsText)
        asJson(metaGraph.add(rootNode, nxv.original_source, res.value.source.removeKeys(metaKeys: _*).noSpaces))
      else
        asJson(metaGraph).map(metaJson => res.value.source.removeKeys(keysToRemove: _*) deepMerge metaJson)
    }
    transformed match {
      case Left(err) =>
        logger.error(
          s"Could not convert resource with id '${res.id}' and value '${res.value}' from Graph back to json. Reason: '${err.message}'"
        )
        None
      case Right(value) =>
        Some(BulkOp.Index(view.index, res.id.value.asString, value.removeKeys("@context")))
    }
  }
}

object ElasticSearchIndexer {

  // $COVERAGE-OFF$
  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  final def start[F[_]: Timer](
      view: ElasticSearchView,
      resources: Resources[F],
      project: Project,
      restartOffset: Boolean
  )(
      implicit client: ElasticSearchClient[F],
      as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      config: AppConfig,
      P: Projections[F, Event],
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {

    val elasticErrorMonadError            = ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
    implicit val p: Project               = project
    implicit val indexing: IndexingConfig = config.elasticSearch.indexing

    val mapper = new ElasticSearchIndexerMapping(view, resources)
    val init =
      for {
        _ <- view.createIndex[F]
        _ <- if (view.rev > 1) client.deleteIndex(view.copy(rev = view.rev - 1).index) else F.pure(true)
      } yield ()

    val processedEventsGauge = Kamon
      .gauge("kg_indexer_gauge")
      .withTag("type", "elasticsearch")
      .withTag("project", project.projectLabel.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCounter = Kamon
      .counter("kg_indexer_counter")
      .withTag("type", "elasticsearch")
      .withTag("project", project.projectLabel.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCount = AtomicLong(0L)

    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name(s"elasticSearch-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .actorOf(actorInitializer)
        .plugin(config.persistence.queryJournalPlugin)
        .retry[ElasticSearchServerOrUnexpectedFailure](indexing.retry.retryStrategy)(elasticErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .mapping(mapper.apply)
        .index(inserts => client.bulk(inserts.removeDupIds))
        .mapInitialProgress { p =>
          processedEventsCount.set(p.processedCount)
          processedEventsGauge.update(p.processedCount.toDouble)
          F.unit
        }
        .mapProgress { p =>
          val previousCount = processedEventsCount.get()
          processedEventsGauge.update(p.processedCount.toDouble)
          processedEventsCounter.increment(p.processedCount - previousCount)
          processedEventsCount.set(p.processedCount)
          F.unit
        }
        .build
    )
  }
  // $COVERAGE-ON$

  private[indexing] val ctx: Json = jsonContentOf("/elasticsearch/default-context.json")
}
