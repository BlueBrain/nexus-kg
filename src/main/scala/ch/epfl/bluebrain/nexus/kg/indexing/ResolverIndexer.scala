package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.AskTimeoutException
import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.{Resolution, Resolver}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, ProjectRef, Rejection, Repo}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexes project resolver events.
  *
  * @param projects    the project operations
  * @param resolution  the function used to derive a resolution for the corresponding project
  */
class ResolverIndexer[F[_]: Repo](
    projects: Projects[F],
    resolution: ProjectRef => Resolution[F]
)(implicit F: MonadError[F, Throwable]) {

  /**
    * Indexes the resolver which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a resolver the event is dropped silently.
    *
    * @param event the event to index
    */
  def apply(event: Event): F[Unit] = {
    val projectRef                  = event.id.parent
    implicit val res: Resolution[F] = resolution(projectRef)

    val result: EitherT[F, Rejection, Boolean] = for {
      resource     <- get(event.id).toRight[Rejection](NotFound(event.id.ref))
      materialized <- materialize(resource)
      resolver     <- EitherT.fromOption(Resolver(materialized), NotFound(event.id.ref))
      applied      <- EitherT.liftF(projects.applyResolver(projectRef, resolver, event.instant))
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
      .map(_ => ())
  }
}

object ResolverIndexer {

  /**
    * Starts the index process for resolvers across all projects in the system.
    *
    * @param projects    the project operations
    * @param resolution  the function used to derive a resolution for the corresponding project
    * @param pluginId    the persistence query plugin id to query the event log
    */
  final def start(
      projects: Projects[Task],
      resolution: ProjectRef => Resolution[Task],
      pluginId: String
  )(implicit repo: Repo[Task], as: ActorSystem, s: Scheduler): ActorRef = {
    val indexer = new ResolverIndexer[Task](projects, resolution)
    SequentialTagIndexer.startLocal[Event](
      (ev: Event) => indexer(ev).runAsync,
      pluginId,
      tag = s"type=${nxv.Resolver.value.show}",
      name = "resolver-indexer"
    )
  }
}
