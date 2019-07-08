package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import cats.effect.{Async, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.SingleView
import ch.epfl.bluebrain.nexus.kg.indexing.ViewStatistics
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectRef}

/**
  * ProjectViewCoordinator backed by [[ProjectViewCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param cache the cache
  * @param ref   the underlying actor reference
  * @tparam F the effect type
  */
class ProjectViewCoordinator[F[_]](cache: Caches[F], ref: ActorRef)(implicit config: AppConfig, F: Async[F]) {

  /**
    * Fetches view statistics for a given view.
    *
    * @param project  project to which the view belongs.
    * @param view     the view to fetch the statistics for.
    * @return [[ViewStatistics]] wrapped in [[F]]
    */
  def viewStatistics(project: Project, view: SingleView): F[ViewStatistics] = {
    implicit val timeout: Timeout = config.sourcing.askTimeout
    IO.fromFuture(IO(ref ? FetchProgress(project.uuid, view))).to[F].map {
      case p: ViewProgress =>
        ViewStatistics(
          processedEvents = p.processedEvents,
          discardedEvents = p.discardedEvents,
          totalEvents = p.totalEvents,
          lastEventDateTime = p.lastEvent,
          lastProcessedEventDateTime = p.lastProcessedEvent
        )
    }
  }

  /**
    * Starts the project view coordinator for the provided project sending a Start message to the
    * underlying coordinator actor.
    * The coordinator actor will attempt to fetch the views linked to the current project and start them
    * while start listening to messages coming from the view cache and the coordinator itself
    *
    * @param project the project for which the view coordinator is triggered
    */
  def start(project: Project): F[Unit] =
    cache.view.getBy[SingleView](project.ref).map { views =>
      ref ! Start(project.uuid, project, views)
    }

  /**
    * Stops the coordinator children views actors and indices related to all the projects
    * that belong to the provided organization.
    *
    * @param orgRef the organization unique identifier
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    cache.project.list(orgRef).flatMap(projects => projects.map(project => stop(project.ref)).sequence) *> F.unit

  /**
    * Stops the coordinator children view actors and indices that belong to the provided organization.
    *
    * @param projectRef the project unique identifier
    */
  def stop(projectRef: ProjectRef): F[Unit] = {
    ref ! Stop(projectRef.id)
    F.unit
  }

  /**
    * Notifies the underlying coordinator actor about a change ocurring to the Project
    * whenever this change is relevant to the coordinator
    *
    * @param newProject the new uncoming project
    * @param project    the previous state of the project
    */
  def change(newProject: Project, project: Project): F[Unit] = {
    if (newProject.label != project.label || newProject.organizationLabel != project.organizationLabel)
      ref ! ProjectChanges(newProject.uuid, newProject)
    F.unit
  }
}
