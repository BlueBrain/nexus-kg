package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.Timer
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{IndexingConfig, IndexingConfigs, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig
import ch.epfl.bluebrain.nexus.sourcing.persistence.OffsetStorage.Volatile
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, ProjectionProgress, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

private class ResolverIndexerMapping[F[_]: Timer](resources: Resources[F])(implicit projectCache: ProjectCache[F],
                                                                           F: MonadError[F, Throwable],
                                                                           indexing: IndexingConfig) {

  private implicit val retry: Retry[F, Throwable] = Retry(indexing.retry.retryStrategy)
  private implicit val log                        = Logger[this.type]

  /**
    * Fetches the resolver which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a resolver the event is dropped silently.
    *
    * @param event event to be mapped to a resolver
    */
  def apply(event: Event): F[Option[Resolver]] =
    fetchProject(event.id.parent).flatMap { implicit project =>
      resources.fetch(event.id, None).value.flatMap {
        case Some(resource) =>
          resources.materialize(resource).value.map {
            case Left(err) =>
              log.error(s"Error on event '${event.id.show} (rev = ${event.rev})', cause: '${err.msg}'")
              None
            case Right(materialized) => Resolver(materialized)
          }
        case _ => F.pure(None)
      }
    }
}

object ResolverIndexer {

  /**
    * Starts the index process for resolvers across all projects in the system.
    *
    * @param resources     the resources operations
    * @param resolverCache the distributed cache
    */
  // $COVERAGE-OFF$
  final def start(resources: Resources[Task], resolverCache: ResolverCache[Task])(
      implicit
      projectCache: ProjectCache[Task],
      as: ActorSystem,
      s: Scheduler,
      persistence: PersistenceConfig,
      indexingCollection: IndexingConfigs,
      sourcingConfig: SourcingConfig): StreamCoordinator[Task, ProjectionProgress] = {

    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError

    implicit val indexing = indexingCollection.keyValueStore

    val mapper = new ResolverIndexerMapping[Task](resources)
    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name("resolver-indexer")
        .tag(s"type=${nxv.Resolver.value.show}")
        .plugin(persistence.queryJournalPlugin)
        .retry[KgError](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(resolverCache.put) *> Task.unit)
        .build)
  }
  // $COVERAGE-ON$
}
