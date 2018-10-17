package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.AskTimeoutException
import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{IndexingConfig, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{AccountNotFound, NotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage.Volatile
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexes project resolver events.
  *
  * @param resources the resources operations
  * @param cache the distributed cache
  */
private class ResolverIndexer[F[_]](resources: Resources[F], cache: DistributedCache[F])(
    implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  /**
    * Indexes the resolver which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a resolver the event is dropped silently.
    *
    * @param event the event to index
    */
  def apply(event: Event): F[Unit] = {
    val projectRef = event.id.parent

    val result: EitherT[F, Rejection, Unit] = for {
      resource     <- resources.fetch(event.id, None).toRight[Rejection](NotFound(event.id.ref))
      materialized <- resources.materialize(resource)
      accountRef   <- EitherT.fromOptionF(cache.accountRef(projectRef), AccountNotFound(projectRef))
      resolver     <- EitherT.fromOption(Resolver(materialized, accountRef), NotFound(event.id.ref))
      applied      <- EitherT.liftF(cache.applyResolver(projectRef, resolver))
    } yield applied

    result.value
      .recoverWith {
        case _: AskTimeoutException =>
          val msg = s"TimedOut while attempting to index resolver event '${event.id.show} (rev = ${event.rev})'"
          F.raiseError(new RetriableErr(msg))
        case OperationTimedOut(reason) =>
          val msg =
            s"TimedOut while attempting to index resolver event '${event.id.show} (rev = ${event.rev})', cause: $reason"
          F.raiseError(new RetriableErr(msg))
      }
      .flatMap {
        case Right(_)                                  => F.pure(())
        case Left(err @ AccountNotFound(`projectRef`)) => F.raiseError(new RetriableErr(err.msg))
        case Left(err) =>
          logger.error(
            s"Error while attempting to fetch/resolve event '${event.id.show} (rev = ${event.rev})', cause: '${err.message}'")
          F.pure(())
      }
  }
}

object ResolverIndexer {

  /**
    * Starts the index process for resolvers across all projects in the system.
    *
    * @param resources the resources operations
    * @param cache the distributed cache
    */
  // $COVERAGE-OFF$
  final def start(resources: Resources[Task], cache: DistributedCache[Task])(implicit
                                                                             as: ActorSystem,
                                                                             s: Scheduler,
                                                                             persistence: PersistenceConfig,
                                                                             indexing: IndexingConfig): ActorRef = {

    val indexer = new ResolverIndexer[Task](resources, cache)
    SequentialTagIndexer.start(
      IndexerConfig.builder
        .name("resolver-indexer")
        .tag(s"type=${nxv.Resolver.value.show}")
        .plugin(persistence.queryJournalPlugin)
        .retry(indexing.retry.maxCount, indexing.retry.strategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .index((l: List[Event]) => Task.sequence(l.removeDupIds.map(indexer(_))).map(_ => ()).runAsync)
        .build)
  }
  // $COVERAGE-ON$
}
