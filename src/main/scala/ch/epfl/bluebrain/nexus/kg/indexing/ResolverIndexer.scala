package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.AskTimeoutException
import cats.MonadError
import cats.data.EitherT
import cats.effect.Timer
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{IndexingConfig, PersistenceConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, OrganizationNotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage.Volatile
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexes project resolver events.
  *
  * @param resources     the resources operations
  * @param resolverCache the distributed cache
  */
private class ResolverIndexer[F[_]: Timer](
    resources: Resources[F],
    resolverCache: ResolverCache[F],
    projectCache: ProjectCache[F])(implicit F: MonadError[F, Throwable], indexing: IndexingConfig) {

  private implicit val retry: Retry[F, Throwable] = Retry(indexing.retry.retryStrategy)
  private val logger                              = Logger[this.type]

  /**
    * Indexes the resolver which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a resolver the event is dropped silently.
    *
    * @param event the event to index
    */
  def apply(event: Event): F[Unit] = {
    val projectRef = event.id.parent
    val result: EitherT[F, Rejection, Unit] = for {
      resource <- resources.fetch(event.id, None).toRight[Rejection](NotFound(event.id.ref))
      project <- EitherT.right(
        projectCache
          .get(projectRef)
          .mapRetry({ case Some(p) => p }, KgError.NotFound(Some(projectRef.show)): Throwable))
      materialized <- resources.materialize(resource)(project)
      resolver     <- EitherT.fromOption(Resolver(materialized), NotFound(event.id.ref))
      applied      <- EitherT.liftF(resolverCache.put(resolver))
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
        case Right(_)                                       => F.pure(())
        case Left(err @ OrganizationNotFound(`projectRef`)) => F.raiseError(new RetriableErr(err.msg))
        case Left(err) =>
          logger.error(
            s"Error while attempting to fetch/resolve event '${event.id.show} (rev = ${event.rev})', cause: '${err.msg}'")
          F.pure(())
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
  final def start(resources: Resources[Task], resolverCache: ResolverCache[Task], projectCache: ProjectCache[Task])(
      implicit
      as: ActorSystem,
      s: Scheduler,
      persistence: PersistenceConfig,
      indexing: IndexingConfig): ActorRef = {

    import ch.epfl.bluebrain.nexus.kg.instances.retriableMonadError

    val indexer = new ResolverIndexer[Task](resources, resolverCache, projectCache)
    SequentialTagIndexer.start(
      IndexerConfig.builder
        .name("resolver-indexer")
        .tag(s"type=${nxv.Resolver.value.show}")
        .plugin(persistence.queryJournalPlugin)
        .retry[RetriableErr](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .index((l: List[Event]) => Task.sequence(l.removeDupIds.map(indexer(_))) *> Task.unit)
        .build)
  }
  // $COVERAGE-ON$
}
