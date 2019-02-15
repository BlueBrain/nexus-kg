package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.MonadError
import cats.effect.Timer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{IndexingConfig, IndexingConfigCollection, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage.Volatile
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

private class ViewIndexerMapping[F[_]: Timer](resources: Resources[F], projectCache: ProjectCache[F])(
    implicit F: MonadError[F, Throwable],
    indexing: IndexingConfig) {

  private implicit val retry: Retry[F, Throwable] = Retry(indexing.retry.retryStrategy)
  private implicit val log                        = Logger[this.type]

  private def fetchProject(projectRef: ProjectRef): F[Project] =
    projectCache
      .get(projectRef)
      .mapRetry({ case Some(p) => p }, KgError.NotFound(Some(projectRef.show)): Throwable)

  /**
    * Fetches the view which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a view the event is dropped silently.
    *
    * @param event event to be mapped to a view
    */
  def apply(event: Event): F[Option[View]] =
    fetchProject(event.id.parent).flatMap { implicit project =>
      resources.fetch(event.id, None).value.flatMap {
        case Some(resource) =>
          resources.materialize(resource).value.map {
            case Left(err) =>
              log.error(s"Error on event '${event.id.show} (rev = ${event.rev})', cause: '${err.msg}'")
              None
            case Right(materialized) => View(materialized).toOption
          }
        case _ => F.pure(None)
      }
    }
}

object ViewIndexer {

  /**
    * Starts the index process for views across all projects in the system.
    *
    * @param resources the resources operations
    * @param viewCache the distributed cache
    */
  // $COVERAGE-OFF$
  final def start(resources: Resources[Task], viewCache: ViewCache[Task], projectCache: ProjectCache[Task])(
      implicit as: ActorSystem,
      s: Scheduler,
      persistence: PersistenceConfig,
      indexingCollection: IndexingConfigCollection): ActorRef = {

    import ch.epfl.bluebrain.nexus.kg.instances.retriableMonadError
    implicit val indexing = indexingCollection.keyValueStore

    val mapper = new ViewIndexerMapping[Task](resources, projectCache)
    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name("view-indexer")
        .tag(s"type=${nxv.View.value.show}")
        .plugin(persistence.queryJournalPlugin)
        .retry[RetriableErr](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(viewCache.put) *> Task.unit)
        .build)
  }
  // $COVERAGE-ON$
}
