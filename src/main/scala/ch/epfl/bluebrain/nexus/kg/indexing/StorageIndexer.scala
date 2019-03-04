package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.MonadError
import cats.effect.Timer
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, StorageCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{IndexingConfig, IndexingConfigs, PersistenceConfig, StorageConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.sourcing.persistence.OffsetStorage.Volatile
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

private class StorageIndexerMapping[F[_]: Timer](resources: Resources[F])(implicit projectCache: ProjectCache[F],
                                                                          F: MonadError[F, Throwable],
                                                                          indexing: IndexingConfig,
                                                                          stConfig: StorageConfig) {

  private implicit val retry: Retry[F, Throwable] = Retry(indexing.retry.retryStrategy)
  private implicit val log                        = Logger[this.type]

  /**
    * Fetches the storage which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a storage the event is dropped silently.
    *
    * @param event event to be mapped to a storage
    */
  def apply(event: Event): F[Option[Storage]] =
    fetchProject(event.id.parent).flatMap { implicit project =>
      resources.fetch(event.id, None).value.flatMap {
        case Some(resource) =>
          resources.materialize(resource).value.map {
            case Left(err) =>
              log.error(s"Error on event '${event.id.show}' (rev = ${event.rev})', cause: '${err.msg}'")
              None
            case Right(materialized) =>
              Storage.apply(materialized) match {
                case Left(err) =>
                  log.error(s"Error on converting resource from event '${event.id.show}' to storage. Reason: '$err'")
                  None
                case Right(storage) => Some(storage)
              }
          }
        case _ => F.pure(None)
      }
    }
}

object StorageIndexer {

  /**
    * Starts the index process for storages across all projects in the system.
    *
    * @param resources    the resources operations
    * @param storageCache the distributed cache for storages
    */
  // $COVERAGE-OFF$
  final def start(resources: Resources[Task], storageCache: StorageCache[Task])(
      implicit
      projectCache: ProjectCache[Task],
      as: ActorSystem,
      s: Scheduler,
      persistence: PersistenceConfig,
      indexingCollection: IndexingConfigs,
      storageConfig: StorageConfig): ActorRef = {

    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError

    implicit val indexing = indexingCollection.keyValueStore

    val mapper = new StorageIndexerMapping[Task](resources)
    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name("storage-indexer")
        .tag(s"type=${nxv.Storage.value.show}")
        .plugin(persistence.queryJournalPlugin)
        .retry[KgError](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(storageCache.put) *> Task.unit)
        .build)
  }
  // $COVERAGE-ON$
}
