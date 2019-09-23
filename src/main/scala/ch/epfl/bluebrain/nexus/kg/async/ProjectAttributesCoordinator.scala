package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import cats.effect.{Async, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectAttributesCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Files, OrganizationRef, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.FetchAttributes
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import journal.Logger
import monix.eval.Task

import scala.util.control.NonFatal

/**
  * ProjectAttributesCoordinator backed by [[ProjectAttributesCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param projectCache the project cache
  * @param ref          the underlying actor reference
  * @tparam F the effect type
  */
class ProjectAttributesCoordinator[F[_]](projectCache: ProjectCache[F], ref: ActorRef)(
    implicit config: AppConfig,
    F: Async[F]
) {
  private implicit val timeout: Timeout = config.sourcing.askTimeout
  private val log                       = Logger[this.type]

  /**
    * Fetches attributes statistics for a given project.
    *
    * @param project  the project
    * @return [[Statistics]] wrapped in [[F]]
    */
  def statistics(project: Project): F[Statistics] = {
    lazy val label = project.projectLabel.show
    IO.fromFuture(IO(ref ? FetchProgress(project.uuid)))
      .to[F]
      .flatMap[Statistics] {
        case s: Statistics => F.pure(s)
        case other =>
          val msg =
            s"Received unexpected reply from the project attributes coordinator actor: '$other' for project '$label'."
          log.error(msg)
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith {
        case _: AskTimeoutException =>
          F.raiseError(
            KgError.OperationTimedOut(
              s"Timeout when asking for statistics to project attributes coordinator for project '$label'"
            )
          )
        case NonFatal(th) =>
          val msg =
            s"Exception caught while exchanging messages with the project attributes coordinator for project '$label'"
          log.error(msg, th)
          F.raiseError(KgError.InternalError(msg))
      }
  }

  /**
    * Starts the project attributes coordinator for the provided project sending a Start message to the
    * underlying coordinator actor.
    * The coordinator actor will start the attributes linked to the current project
    *
    * @param project the project for which the attributes coordinator is triggered
    */
  def start(project: Project): F[Unit] = {
    ref ! Start(project.uuid, project)
    F.unit
  }

  /**
    * Stops the coordinator children attributes actor for all the projects that belong to the provided organization.
    *
    * @param orgRef the organization unique identifier
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    projectCache.list(orgRef).flatMap(projects => projects.map(project => stop(project.ref)).sequence) *> F.unit

  /**
    * Stops the coordinator children attributes actor for the provided project
    *
    * @param projectRef the project unique identifier
    */
  def stop(projectRef: ProjectRef): F[Unit] = {
    ref ! Stop(projectRef.id)
    F.unit
  }
}

object ProjectAttributesCoordinator {
  def apply(files: Files[Task], projectCache: ProjectCache[Task])(
      implicit config: AppConfig,
      fetchAttributes: FetchAttributes[Task],
      as: ActorSystem,
      P: Projections[Task, Event]
  ): ProjectAttributesCoordinator[Task] = {
    val coordinatorRef = ProjectAttributesCoordinatorActor.start(files, None, config.cluster.shards)
    new ProjectAttributesCoordinator[Task](projectCache, coordinatorRef)
  }
}
