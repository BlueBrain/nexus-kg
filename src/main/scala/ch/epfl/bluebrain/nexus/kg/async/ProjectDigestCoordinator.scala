package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import cats.effect.{Async, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.async.ProjectDigestCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Files, OrganizationRef, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.FetchDigest
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import monix.eval.Task

/**
  * ProjectDigestCoordinator backed by [[ProjectDigestCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param projectCache the project cache
  * @param ref          the underlying actor reference
  * @tparam F the effect type
  */
class ProjectDigestCoordinator[F[_]](projectCache: ProjectCache[F], ref: ActorRef)(
    implicit config: AppConfig,
    F: Async[F]
) {
  private implicit val timeout: Timeout = config.sourcing.askTimeout

  /**
    * Fetches digest statistics for a given project.
    *
    * @param project  the project
    * @return [[Statistics]] wrapped in [[F]]
    */
  def statistics(project: Project): F[Statistics] =
    IO.fromFuture(IO(ref ? FetchProgress(project.uuid))).to[F].map { case s: Statistics => s }

  /**
    * Starts the project digest coordinator for the provided project sending a Start message to the
    * underlying coordinator actor.
    * The coordinator actor will start the digest linked to the current project
    *
    * @param project the project for which the view coordinator is triggered
    */
  def start(project: Project): F[Unit] = {
    ref ! Start(project.uuid, project)
    F.unit
  }

  /**
    * Stops the coordinator children digest actor for all the projects that belong to the provided organization.
    *
    * @param orgRef the organization unique identifier
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    projectCache.list(orgRef).flatMap(projects => projects.map(project => stop(project.ref)).sequence) *> F.unit

  /**
    * Stops the coordinator children digest actor for the provided project
    *
    * @param projectRef the project unique identifier
    */
  def stop(projectRef: ProjectRef): F[Unit] = {
    ref ! Stop(projectRef.id)
    F.unit
  }
}

object ProjectDigestCoordinator {
  def apply(files: Files[Task], projectCache: ProjectCache[Task])(
      implicit config: AppConfig,
      fetchDigest: FetchDigest[Task],
      as: ActorSystem,
      P: Projections[Task, Event]
  ): ProjectDigestCoordinator[Task] = {
    val coordinatorRef = ProjectDigestCoordinatorActor.start(files, None, config.cluster.shards)
    new ProjectDigestCoordinator[Task](projectCache, coordinatorRef)
  }
}
