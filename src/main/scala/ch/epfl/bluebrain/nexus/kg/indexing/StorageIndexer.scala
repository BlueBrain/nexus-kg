package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, StorageCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage.Volatile
import ch.epfl.bluebrain.nexus.sourcing.projections._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import journal.Logger

private class StorageIndexerMapping[F[_]: Timer](resources: Resources[F])(implicit projectCache: ProjectCache[F],
                                                                          F: MonadError[F, Throwable],
                                                                          indexing: IndexingConfig,
                                                                          stConfig: StorageConfig) {

  private implicit val retry: Retry[F, Throwable] = Retry[F, Throwable](indexing.retry.retryStrategy)
  private implicit val log: Logger                = Logger[this.type]

  /**
    * Fetches the storage which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a storage the event is dropped silently.
    *
    * @param event event to be mapped to a storage
    */
  def apply(event: Event): F[Option[Storage]] =
    fetchProject(event.id.parent).flatMap { implicit project =>
      resources.fetch(event.id).value.flatMap {
        case Right(resource) =>
          resources.materialize(resource).value.map {
            case Left(err) =>
              log.error(s"Error on event '${event.id.show}' (rev = ${event.rev})', cause: '${err.msg}'")
              None
            case Right(materialized) =>
              Storage.apply(materialized, encrypt = false) match {
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

  // $COVERAGE-OFF$
  /**
    * Starts the index process for storages across all projects in the system.
    *
    * @param resources    the resources operations
    * @param storageCache the distributed cache for storages
    */
  final def start[F[_]: Timer](resources: Resources[F], storageCache: StorageCache[F])(
      implicit
      projectCache: ProjectCache[F],
      as: ActorSystem,
      config: AppConfig,
      F: Effect[F],
  ): StreamSupervisor[F, ProjectionProgress] = {

    val kgErrorMonadError = ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError

    implicit val indexing: IndexingConfig = config.keyValueStore.indexing

    val mapper = new StorageIndexerMapping[F](resources)
    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name("storage-indexer")
        .tag(s"type=${nxv.Storage.value.show}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[KgError](indexing.retry.retryStrategy)(kgErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(storageCache.put)(F) >> F.unit)
        .build)
  }

  /**
    * Starts the index process for storages across all projects in the system.
    *
    * @param resources    the resources operations
    * @param storageCache the distributed cache for storages
    */
  final def delay[F[_]: Timer: Effect](resources: Resources[F], storageCache: StorageCache[F])(
      implicit
      projectCache: ProjectCache[F],
      as: ActorSystem,
      config: AppConfig,
  ): F[StreamSupervisor[F, ProjectionProgress]] =
    Effect[F].delay(start(resources, storageCache))
  // $COVERAGE-ON$
}
