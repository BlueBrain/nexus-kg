package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.AskTimeoutException
import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PersistenceConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexes project view events.
  *
  * @param resources the resources operations
  * @param cache the distributed cache
  */
private class ViewIndexer[F[_]](resources: Resources[F], cache: DistributedCache[F])(
    implicit F: MonadError[F, Throwable]) {

  /**
    * Indexes the view which corresponds to the argument event. If the resource is not found, or it's not compatible to
    * a view the event is dropped silently.
    *
    * @param event the event to index
    */
  def apply(event: Event): F[Unit] = {
    val projectRef = event.id.parent

    val result: EitherT[F, Rejection, Boolean] = for {
      resource     <- resources.fetch(event.id, None).toRight[Rejection](NotFound(event.id.ref))
      materialized <- resources.materialize(resource)
      view         <- EitherT.fromOption(View(materialized), NotFound(event.id.ref))
      applied      <- EitherT.liftF(cache.applyView(projectRef, view))
    } yield applied

    result.value
      .recoverWith {
        case _: AskTimeoutException =>
          val msg = s"TimedOut while attempting to index view event '${event.id.show} (rev = ${event.rev})'"
          F.raiseError(new RetriableErr(msg))
        case OperationTimedOut(reason) =>
          val msg =
            s"TimedOut while attempting to index view event '${event.id.show} (rev = ${event.rev})', cause: $reason"
          F.raiseError(new RetriableErr(msg))
      }
      .map(_ => ())
  }
}

object ViewIndexer {

  /**
    * Starts the index process for views across all projects in the system.
    *
    * @param resources the resources operations
    * @param cache     the distributed cache
    */
  // $COVERAGE-OFF$
  final def start(resources: Resources[Task], cache: DistributedCache[Task])(
      implicit as: ActorSystem,
      s: Scheduler,
      persistence: PersistenceConfig): ActorRef = {
    val indexer = new ViewIndexer[Task](resources, cache)
    SequentialTagIndexer.startLocal[Event](
      (ev: Event) => indexer(ev).runAsync,
      persistence.queryJournalPlugin,
      tag = s"type=${nxv.View.value.show}",
      name = "view-indexer"
    )
  }
  // $COVERAGE-ON$
}
