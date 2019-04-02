package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.Monad
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults, SparqlWriteQuery}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, ProjectionProgress, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator
import journal.Logger
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong

import scala.collection.JavaConverters._

private class SparqlIndexerMapping[F[_]](view: SparqlView, resources: Resources[F])(implicit F: Monad[F],
                                                                                    project: Project) {

  private val logger: Logger = Logger[this.type]

  /**
    * When an event is received, the current state is obtained and a [[SparqlWriteQuery]] is built.
    *
    * @param event event to be mapped to a Sparql insert query
    */
  final def apply(event: Event): F[Option[Identified[ProjectRef, SparqlWriteQuery]]] =
    view.resourceTag.map(resources.fetch(event.id, _)).getOrElse(resources.fetch(event.id)).value.flatMap {
      case Right(resource) if validCandidate(resource) => buildInsertQuery(resource)
      case _                                           => F.pure(None)
    }

  private def validCandidate(resource: Resource): Boolean =
    view.resourceSchemas.isEmpty || view.resourceSchemas.contains(resource.schema.iri)

  private def buildInsertQuery(res: Resource): F[Option[Identified[ProjectRef, SparqlWriteQuery]]] =
    resources.materializeWithMeta(res, selfAsIri = true).value.map {
      case Left(e) =>
        logger.error(s"Unable to materialize with meta, due to '$e'")
        None
      case Right(r) =>
        val graph = if (view.includeMetadata) r.value.graph else r.value.graph.removeMetadata
        Some(res.id -> SparqlWriteQuery.replace(toGraphUri(res.id), graph))
    }

  private def toGraphUri(id: ResId): Uri = (id.value + "graph").toAkkaUri
}

object SparqlIndexer {

  /**
    * Starts the index process for an sparql client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  // $COVERAGE-OFF$
  final def start(view: SparqlView, resources: Resources[Task], project: Project, restartOffset: Boolean)(
      implicit as: ActorSystem,
      ul: UntypedHttpClient[Task],
      s: Scheduler,
      uclRs: HttpClient[Task, SparqlResults],
      config: AppConfig): StreamCoordinator[Task, ProjectionProgress] = {

    import ch.epfl.bluebrain.nexus.kg.instances.sparqlErrorMonadError
    implicit val lb       = project
    implicit val indexing = config.sparql.indexing

    val properties: Map[String, String] = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/blazegraph/index.properties"))
      props.asScala.toMap
    }

    val client = BlazegraphClient[Task](config.sparql.base, view.index, config.sparql.akkaCredentials)
    val mapper = new SparqlIndexerMapping(view, resources)
    val init =
      for {
        _ <- client.createNamespace(properties)
        _ <- if (view.rev > 1) client.copy(namespace = view.copy(rev = view.rev - 1).index).deleteNamespace
        else Task.pure(true)
      } yield ()

    val processedEventsGauge = Kamon
      .gauge("kg_indexer_gauge")
      .refine(
        "type"         -> "sparql",
        "project"      -> project.projectLabel.show,
        "organization" -> project.organizationLabel,
        "viewId"       -> view.id.show
      )
    val processedEventsCounter = Kamon
      .counter("kg_indexer_counter")
      .refine(
        "type"         -> "sparql",
        "project"      -> project.projectLabel.show,
        "organization" -> project.organizationLabel,
        "viewId"       -> view.id.show
      )
    val processedEventsCount = AtomicLong(0L)

    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name(s"sparql-indexer-${view.index}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[SparqlServerOrUnexpectedFailure](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .mapping(mapper.apply)
        .index(inserts => client.bulk(inserts.removeDupIds: _*))
        .mapInitialProgress { p =>
          processedEventsCount.set(p.processedCount)
          processedEventsGauge.set(p.processedCount)
          Task.unit
        }
        .mapProgress { p =>
          val previousCount = processedEventsCount.get()
          processedEventsGauge.set(p.processedCount)
          processedEventsCounter.increment(p.processedCount - previousCount)
          processedEventsCount.set(p.processedCount)
          Task.unit
        }
        .build)
  }
  // $COVERAGE-ON$
}
