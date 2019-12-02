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
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, IndexedView}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{IdentifiedValue, OrganizationRef, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * ProjectViewCoordinator backed by [[ProjectViewCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param cache the cache
  * @param ref   the underlying actor reference
  * @tparam F the effect type
  */
@SuppressWarnings(Array("ListSize"))
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
    * @param viewId the view unique identifier on a project
    * @return Some(statistics) if view exists, None otherwise wrapped in [[F]]
    */
  def statistics(viewId: AbsoluteIri)(implicit project: Project): F[Option[Statistics]] = {
    val msgF = IO.fromFuture(IO(ref ? FetchStatistics(project.uuid, viewId))).to[F]
    parse[Statistics](msgF).recoverWith(logAndRaiseError(project.show, "statistics"))
  }

  /**
    * Fetches view statistics for a given projection.
    *
    * @param viewId       the view unique identifier on a project
    * @param projectionId the projection unique identifier on the target view
    * @return Some(statistics) if projection exists, None otherwise wrapped in [[F]]
    */
  def statistics(viewId: AbsoluteIri, projectionId: AbsoluteIri)(implicit project: Project): F[Option[Statistics]] = {
    val msgF = IO.fromFuture(IO(ref ? FetchProjectionStatistics(project.uuid, viewId, projectionId))).to[F]
    parse[Statistics](msgF).recoverWith(logAndRaiseError(project.show, "statistics"))
  }

  /**
    * Fetches view offset.
    *
    * @param viewId the view unique identifier on a project
    * @return Some(offset) if view exists, None otherwise wrapped in [[F]]
    */
  def offset(viewId: AbsoluteIri)(implicit project: Project): F[Option[Offset]] = {
    val msgF = IO.fromFuture(IO(ref ? FetchOffset(project.uuid, viewId))).to[F]
    parse[Offset](msgF).recoverWith(logAndRaiseError(project.show, "progress"))
  }

  /**
    * Fetches view offset for a given projection.
    *
    * @param viewId       the view unique identifier on a project
    * @param projectionId the projection unique identifier on the target view
    * @return Some(offset) if projection exists, None otherwise wrapped in [[F]]
    */
  def offset(viewId: AbsoluteIri, projectionId: AbsoluteIri)(implicit project: Project): F[Option[Offset]] = {
    val msgF = IO.fromFuture(IO(ref ? FetchProjectionOffset(project.uuid, viewId, projectionId))).to[F]
    parse[Offset](msgF).recoverWith(logAndRaiseError(project.show, "progress"))
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
      .map(projection => statistics(view.id, projection.view.id).map(IdentifiedValue(projection.view.id, _)))
      .sequence
      .map { results =>
        val collected = results.collect { case IdentifiedValue(id, Some(value)) => IdentifiedValue(id, value) }
        UnscoredQueryResults(collected.size.toLong, collected.map(UnscoredQueryResult.apply))
      }

  /**
    * Fetches progress for all projections inside a composite view.
    *
    * @param view the view to fetch the statistics for
    * @return [[QueryResults[Offset]]] wrapped in [[F]]
    */
  def projectionsOffset(view: CompositeView)(implicit project: Project): F[QueryResults[IdentifiedValue[Offset]]] =
    view.projections.toList
      .map(projection => offset(view.id, projection.view.id).map(IdentifiedValue(projection.view.id, _)))
      .sequence
      .map { results =>
        val collected = results.collect { case IdentifiedValue(id, Some(value)) => IdentifiedValue(id, value) }
        UnscoredQueryResults(collected.size.toLong, collected.map(UnscoredQueryResult.apply))
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
  def stop(projectRef: ProjectRef): F[Unit] =
    F.delay(ref ! Stop(projectRef.id))

  /**
    * Triggers restart of a view from the initial progress.
    *
    * @param viewId the view unique identifier on a project
    * @return Some(())) if view exists, None otherwise wrapped in [[F]]
    */
  def restart(viewId: AbsoluteIri)(implicit project: Project): F[Option[Unit]] = {
    val msgF = IO.fromFuture(IO(ref ? RestartView(project.uuid, viewId))).to[F]
    parse[Ack](msgF).recoverWith(logAndRaiseError(project.show, "restart")).map(_.map(_ => ()))
  }

  /**
    * Triggers restart of a composite view where the passed projection will start from the initial progress.
    *
    * @param viewId       the view unique identifier on a project
    * @param projectionId the projection unique identifier on the target view
    * @return Some(())) if projection exists, None otherwise wrapped in [[F]]
    */
  def restart(viewId: AbsoluteIri, projectionId: AbsoluteIri)(implicit project: Project): F[Option[Unit]] = {
    val msgF = IO.fromFuture(IO(ref ? RestartProjection(project.uuid, viewId, projectionId))).to[F]
    parse[Ack](msgF).recoverWith(logAndRaiseError(project.show, "restart")).map(_.map(_ => ()))
  }

  /**
    * Triggers restart of a composite view where all the projection will start from the initial progress.
    *
    * @param project project to which the view belongs
    * @param viewId       the view unique identifier on a project
    * @return Some(())) if view exists, None otherwise wrapped in [[F]]
    */
  def restartProjections(viewId: AbsoluteIri)(implicit project: Project): F[Option[Unit]] = {
    val msgF = IO.fromFuture(IO(ref ? RestartProjections(project.uuid, viewId))).to[F]
    parse[Ack](msgF).recoverWith(logAndRaiseError(project.show, "restart")).map(_.map(_ => ()))
  }

  /**
    * Notifies the underlying coordinator actor about a change occurring to the Project
    * whenever this change is relevant to the coordinator
    *
    * @param newProject the new incoming project
    * @param project    the previous state of the project
    */
  def change(newProject: Project, project: Project): F[Unit] =
    if (newProject.label != project.label || newProject.organizationLabel != project.organizationLabel)
      F.delay(ref ! ProjectChanges(newProject.uuid, newProject))
    else F.unit

  private def parse[A](msgF: F[Any])(implicit A: ClassTag[A], project: Project): F[Option[A]] =
    msgF.flatMap[Option[A]] {
      case Some(A(value)) => F.pure(Some(value))
      case None           => F.pure(None)
      case other =>
        val msg =
          s"Received unexpected reply from the project view coordinator actor: '$other' for project '${project.show}'."
        F.raiseError(KgError.InternalError(msg))
    }

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
