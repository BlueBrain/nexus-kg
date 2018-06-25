package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import cats.MonadError
import cats.data.EitherT
import cats.effect.Timer
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, ProjectRef, Rejection, Repo}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import monix.eval.Task
import monix.execution.Scheduler

/**
  * Indexes project view events.
  *
  * @param projects    the project operations
  * @param resolution  the function used to derive a resolution for the corresponding project
  * @param retryConfig configuration for applying retries in case of failures
  */
class ViewIndexer[F[_]: Repo: Timer](
    projects: Projects[F],
    resolution: ProjectRef => Resolution[F],
    retryConfig: RetryConfig
)(implicit F: MonadError[F, Throwable]) {

  /**
    * Indexes the view which corresponds to the argument event. If the resource is not found, or it's not compatible to
    * a view the event is dropped silently.
    *
    * @param event the event to index
    */
  def apply(event: Event): F[Unit] = {
    val projectRef                  = event.id.parent
    implicit val res: Resolution[F] = resolution(projectRef)

    val result: EitherT[F, Rejection, Boolean] = for {
      resource     <- get(event.id).toRight[Rejection](NotFound(event.id.ref))
      materialized <- materialize(resource)
      view         <- EitherT.fromOption(View(materialized), NotFound(event.id.ref))
      applied      <- EitherT.liftF(projects.applyView(projectRef, view))
    } yield applied

    Retry.retryWithBackOff(result.value, retryConfig).map(_ => ())
  }
}

object ViewIndexer {

  /**
    * Starts the index process for views across all projects in the system.
    *
    * @param projects    the project operations
    * @param resolution  the function used to derive a resolution for the corresponding project
    * @param retryConfig configuration for applying retries in case of failures
    * @param pluginId    the persistence query plugin id to query the event log
    */
  final def start(
      projects: Projects[Task],
      resolution: ProjectRef => Resolution[Task],
      retryConfig: RetryConfig,
      pluginId: String
  )(implicit repo: Repo[Task], as: ActorSystem, s: Scheduler): ActorRef = {
    implicit val timer: Timer[Task] = s.timer
    val indexer                     = new ViewIndexer[Task](projects, resolution, retryConfig)
    SequentialTagIndexer.startLocal[Event](
      (ev: Event) => indexer(ev).runAsync,
      pluginId,
      tag = s"type=${nxv.View.value.show}",
      name = "view-indexer"
    )
  }

}
