package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem, Props}
import cats.Monad
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.sourcing.projections._
import kamon.Kamon
import monix.execution.atomic.AtomicLong

private[indexing] class SparqlIndexerMapping[F[_]: Monad](view: SparqlView, resources: Resources[F])(
    implicit project: Project
) {
  private val metadataOptions = MetadataOptions(linksAsIri = true, expandedLinks = true)

  /**
    * When an event is received, the current state is obtained and a [[SparqlWriteQuery]] is built.
    *
    * @param event event to be mapped to a Sparql insert query
    */
  final def apply(event: Event): F[Option[(ResourceV, SparqlWriteQuery)]] =
    view.filter.resourceTag
      .filter(_.trim.nonEmpty)
      .map(resources.fetch(event.id, _, metadataOptions, None))
      .getOrElse(resources.fetch(event.id, metadataOptions, None))
      .value
      .map {
        case Right(res) if validSchema(view, res) && validTypes(view, res) => Some(res -> buildInsertOrDeleteQuery(res))
        case Right(res) if validSchema(view, res)                          => Some(res -> buildDeleteQuery(res))
        case _                                                             => None
      }

  private def buildInsertOrDeleteQuery(res: ResourceV): SparqlWriteQuery =
    if (res.deprecated && !view.filter.includeDeprecated) buildDeleteQuery(res)
    else buildInsertQuery(res)

  private def buildInsertQuery(res: ResourceV): SparqlWriteQuery = {
    val graph = if (view.includeMetadata) res.value.graph else res.value.graph.removeMetadata
    SparqlWriteQuery.replace(res.id.toGraphUri, graph)
  }

  private def buildDeleteQuery(res: ResourceV): SparqlWriteQuery =
    SparqlWriteQuery.drop(res.id.toGraphUri)
}

@SuppressWarnings(Array("MaxParameters"))
object SparqlIndexer {

  // $COVERAGE-OFF$
  /**
    * Starts the index process for an sparql client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  final def start[F[_]: Timer](view: SparqlView, resources: Resources[F], project: Project, restartOffset: Boolean)(
      implicit as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      P: Projections[F, Event],
      F: Effect[F],
      clients: Clients[F],
      config: AppConfig
  ): StreamSupervisor[F, ProjectionProgress] = {

    val sparqlErrorMonadError             = ch.epfl.bluebrain.nexus.kg.instances.sparqlErrorMonadError
    implicit val indexing: IndexingConfig = config.sparql.indexing
    val client                            = clients.sparql.copy(namespace = view.index)

    val mapper = new SparqlIndexerMapping(view, resources)(sparqlErrorMonadError, project)

    val processedEventsGauge = Kamon
      .gauge("kg_indexer_gauge")
      .withTag("type", "sparql")
      .withTag("project", project.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCounter = Kamon
      .counter("kg_indexer_counter")
      .withTag("type", "sparql")
      .withTag("project", project.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCount = AtomicLong(0L)

    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name(s"sparql-indexer-${view.index}")
        .tag(s"project=${view.ref.id}")
        .actorOf(actorInitializer)
        .plugin(config.persistence.queryJournalPlugin)
        .retry[SparqlServerOrUnexpectedFailure](indexing.retry.retryStrategy)(sparqlErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(view.createIndex)
        .mapping(mapper.apply)
        .index(inserts => client.bulk(inserts.removeDupIds: _*))
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
}
