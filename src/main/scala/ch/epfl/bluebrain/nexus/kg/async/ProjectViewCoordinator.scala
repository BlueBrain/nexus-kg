package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{ask, AskTimeoutException}
import akka.persistence.query.Offset
import akka.util.Timeout
import cats.effect.{Async, ContextShift, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, IndexedView, SingleView}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{IdentifiedValue, OrganizationRef, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
  * ProjectViewCoordinator backed by [[ProjectViewCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param cache the cache
  * @param ref   the underlying actor reference
  * @tparam F the effect type
  */
class ProjectViewCoordinator[F[_]](cache: Caches[F], ref: ActorRef)(
    implicit config: AppConfig,
    F: Async[F],
    ec: ExecutionContext
) {

  private implicit val timeout: Timeout               = config.sourcing.askTimeout
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)
  private val log                                     = Logger[this.type]

  /**
    * Fetches view statistics for a given view.
    *
    * @param view the view to fetch the statistics for
    * @return [[Statistics]] wrapped in [[F]]
    */
  def statistics(view: IndexedView)(implicit project: Project): F[Statistics] = {
    lazy val label = project.show
    IO.fromFuture(IO(ref ? FetchStatistics(project.uuid, view)))
      .to[F]
      .flatMap[Statistics] {
        case value: Statistics => F.pure(value)
        case other =>
          val msg = s"Received unexpected reply from the project view coordinator actor: '$other' for project '$label'."
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith(logAndRaiseError(label, "statistics"))
  }

  /**
    * Fetches view statistics for a given projection.
    *
    * @param view           the composite view where the projection belongs to
    * @param projectionView the view to fetch the statistics for
    * @return [[Statistics]] wrapped in [[F]]
    */
  def statistics(view: CompositeView, projectionView: SingleView)(implicit project: Project): F[Statistics] = {
    lazy val label = project.show
    IO.fromFuture(IO(ref ? FetchProjectionStatistics(project.uuid, view, projectionView)))
      .to[F]
      .flatMap[Statistics] {
        case value: Statistics => F.pure(value)
        case other =>
          val msg = s"Received unexpected reply from the project view coordinator actor: '$other' for project '$label'."
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith(logAndRaiseError(label, "statistics"))
  }

  /**
    * Fetches view offset.
    *
    * @param view the view to fetch the statistics for
    * @return [[Offset]] wrapped in [[F]]
    */
  def offset(view: IndexedView)(implicit project: Project): F[Offset] = {
    lazy val label = project.show
    IO.fromFuture(IO(ref ? FetchOffset(project.uuid, view)))
      .to[F]
      .flatMap[Offset] {
        case value: Offset => F.pure(value)
        case other =>
          val msg = s"Received unexpected reply from the project view coordinator actor: '$other' for project '$label'."
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith(logAndRaiseError(label, "progress"))
  }

  /**
    * Fetches view offset for a given projection.
    *
    * @param view           the composite view where the projection belongs to
    * @param projectionView the view to fetch the offset for
    * @return [[Offset]] wrapped in [[F]]
    */
  def offset(view: CompositeView, projectionView: SingleView)(implicit project: Project): F[Offset] = {
    lazy val label = project.show
    IO.fromFuture(IO(ref ? FetchProjectionOffset(project.uuid, view, projectionView)))
      .to[F]
      .flatMap[Offset] {
        case value: Offset => F.pure(value)
        case other =>
          val msg = s"Received unexpected reply from the project view coordinator actor: '$other' for project '$label'."
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith(logAndRaiseError(label, "progress"))
  }

  /**
    * Fetches statistics for all projections inside a composite view.
    *
    * @param view the view to fetch the statistics for
    * @return [[QueryResults[Statistics]]] wrapped in [[F]]
    */
  def projectionsStatistic(
      view: CompositeView
  )(implicit project: Project): F[QueryResults[IdentifiedValue[Statistics]]] =
    view.projections.toList
      .map(_.view)
      .map(projectionView => statistics(view, projectionView).map(IdentifiedValue(projectionView.id, _)))
      .sequence
      .map(results => UnscoredQueryResults(results.size.toLong, results.map(UnscoredQueryResult.apply)))

  /**
    * Fetches progress for all projections inside a composite view.
    *
    * @param view the view to fetch the statistics for
    * @return [[QueryResults[Offset]]] wrapped in [[F]]
    */
  def projectionsOffset(view: CompositeView)(implicit project: Project): F[QueryResults[IdentifiedValue[Offset]]] =
    view.projections.toList
      .map(_.view)
      .map(projectionView => offset(view, projectionView).map(IdentifiedValue(projectionView.id, _)))
      .sequence
      .map(results => UnscoredQueryResults(results.size.toLong, results.map(UnscoredQueryResult.apply)))

  private def logAndRaiseError[A](label: String, action: String): PartialFunction[Throwable, F[A]] = {
    case _: AskTimeoutException =>
      F.raiseError(
        KgError
          .OperationTimedOut(s"Timeout when asking for $action to project view coordinator for project '$label'")
      )
    case NonFatal(th) =>
      val msg = s"Exception caught while exchanging messages with the project view coordinator for project '$label'"
      log.error(msg, th)
      F.raiseError(KgError.InternalError(msg))
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
    cache.view.getBy[IndexedView](project.ref).map { views =>
      ref ! Start(project.uuid, project, views)
    }

  /**
    * Stops the coordinator children views actors and indices related to all the projects
    * that belong to the provided organization.
    *
    * @param orgRef the organization unique identifier
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    cache.project.list(orgRef).flatMap(projects => projects.map(project => stop(project.ref)).sequence) >> F.unit

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
    * Triggers restart of a view from the initial progress.
    *
    * @param view the view to be restarted
    */
  def restart(view: IndexedView)(implicit project: Project): F[Unit] = {
    ref ! RestartView(project.uuid, view)
    F.unit
  }

  /**
    * Triggers restart of a composite view where the passed projection will start from the initial progress.
    *
    * @param view              the view to be restarted
    * @param restartOffsetView the projection view for which the offset is restarted
    */
  def restart(view: CompositeView, restartOffsetView: SingleView)(implicit project: Project): F[Unit] =
    restart(view, Set(restartOffsetView))

  /**
    * Triggers restart of a composite view where all the projection will start from the initial progress.
    *
    * @param project project to which the view belongs
    * @param view    the view to be restarted
    */
  def restartProjections(view: CompositeView)(implicit project: Project): F[Unit] = {
    ref ! RestartProjections(project.uuid, view, view.projections.map(_.view))
    F.unit
  }

  private def restart(view: CompositeView, restartOffsetViews: Set[SingleView])(implicit project: Project): F[Unit] = {
    ref ! RestartProjections(project.uuid, view, restartOffsetViews)
    F.unit
  }

  /**
    * Notifies the underlying coordinator actor about a change occurring to the Project
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

object ProjectViewCoordinator {
  def apply(resources: Resources[Task], cache: Caches[Task])(
      implicit config: AppConfig,
      as: ActorSystem,
      clients: Clients[Task],
      P: Projections[Task, String]
  ): ProjectViewCoordinator[Task] = {
    val coordinatorRef = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    new ProjectViewCoordinator[Task](cache, coordinatorRef)
  }
}
