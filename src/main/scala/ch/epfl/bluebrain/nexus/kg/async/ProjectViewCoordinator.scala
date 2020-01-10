package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{ask, AskTimeoutException}
import akka.persistence.query.Offset
import akka.util.Timeout
import cats.effect.{Async, ContextShift, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlList, AccessControlLists}
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.{AclsCache, Caches, ProjectCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Source.CrossProjectEventStream
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, IndexedView, SingleView}
import ch.epfl.bluebrain.nexus.kg.indexing.{IdentifiedProgress, Statistics}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import com.typesafe.scalalogging.Logger
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
    aclsCache: AclsCache[F],
    F: Async[F],
    ec: ExecutionContext
) {

  private implicit val timeout: Timeout               = config.sourcing.askTimeout
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)
  private implicit val log: Logger                    = Logger[this.type]
  private implicit val projectCache: ProjectCache[F]  = cache.project

  private type IdStatsSet  = Set[IdentifiedProgress[Statistics]]
  private type IdOffsetSet = Set[IdentifiedProgress[Offset]]

  /**
    * Fetches view statistics for a given view.
    *
    * @param viewId      the view unique identifier on a project
    * @param sourceIdOpt the optional source unique identifier on the target view
    */
  def statistics(viewId: AbsoluteIri, sourceIdOpt: Option[AbsoluteIri])(
      implicit project: Project
  ): F[Option[IdStatsSet]] =
    fetchAllStatistics(viewId)
      .map(_.filter { idStats =>
        idStats.projectionId.isEmpty && sourceIdOpt.forall(idStats.sourceId.contains)
      })
      .map(emptyToNone)

  /**
    * Fetches view statistics for a given projection(s).
    *
    * @param viewId          the view unique identifier on a project
    * @param sourceIdOpt     the optional source unique identifier on the target view
    * @param projectionIdOpt the optional projection unique identifier on the target view
    */
  def projectionStats(viewId: AbsoluteIri, sourceIdOpt: Option[AbsoluteIri], projectionIdOpt: Option[AbsoluteIri])(
      implicit project: Project
  ): F[Option[IdStatsSet]] =
    fetchAllStatistics(viewId)
      .map(_.filter { idStats =>
        idStats.projectionId.isDefined &&
        projectionIdOpt.forall(idStats.projectionId.contains) &&
        sourceIdOpt.forall(idStats.sourceId.contains)
      })
      .map(emptyToNone)

  /**
    * Fetches view offset.
    *
    * @param viewId      the view unique identifier on a project
    * @param sourceIdOpt the optional source unique identifier on the target view
    */
  def offset(viewId: AbsoluteIri, sourceIdOpt: Option[AbsoluteIri])(implicit project: Project): F[Option[IdOffsetSet]] =
    fetchAllOffsets(viewId)
      .map(_.filter { idOffsets =>
        idOffsets.projectionId.isEmpty && sourceIdOpt.forall(idOffsets.sourceId.contains)
      })
      .map(emptyToNone)

  /**
    * Fetches view offset for a given projection(s).
    *
    * @param viewId          the view unique identifier on a project
    * @param sourceIdOpt     the optional source unique identifier on the target view
    * @param projectionIdOpt the optional projection unique identifier on the target view
    */
  def projectionOffset(viewId: AbsoluteIri, sourceIdOpt: Option[AbsoluteIri], projectionIdOpt: Option[AbsoluteIri])(
      implicit project: Project
  ): F[Option[IdOffsetSet]] =
    fetchAllOffsets(viewId)
      .map(_.filter { idOffsets =>
        idOffsets.projectionId.isDefined &&
        projectionIdOpt.forall(idOffsets.projectionId.contains) &&
        sourceIdOpt.forall(idOffsets.sourceId.contains)
      })
      .map(emptyToNone)

  /**
    * Starts the project view coordinator for the provided project sending a Start message to the
    * underlying coordinator actor.
    * The coordinator actor will attempt to fetch the views linked to the current project and start them
    * while start listening to messages coming from the view cache and the coordinator itself
    *
    * @param project the project for which the view coordinator is triggered
    */
  def start(project: Project): F[Unit] =
    cache.view.getBy[IndexedView](project.ref).flatMap {
      case views if containsCrossProject(views) =>
        aclsCache.list.flatMap(resolveProjects(_)).map(projAcls => ref ! Start(project.uuid, project, views, projAcls))
      case views => F.pure(ref ! Start(project.uuid, project, views, Map.empty[Project, AccessControlList]))
    }

  private def containsCrossProject(views: Set[IndexedView]): Boolean =
    views.exists {
      case _: SingleView       => false
      case view: CompositeView => view.sourcesBy[CrossProjectEventStream].nonEmpty
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
    parseOpt[Ack](msgF).recoverWith(logAndRaiseError(project.show, "restart")).map(_.map(_ => ()))
  }

  /**
    * Triggers restart of a composite view where the passed projection will start from the initial progress.
    *
    * @param viewId       the view unique identifier on a project
    * @param sourceIdOpt     the optional source unique identifier on the target view
    * @param projectionIdOpt the optional projection unique identifier on the target view
    * @return Some(())) if projection exists, None otherwise wrapped in [[F]]
    */
  def restart(viewId: AbsoluteIri, sourceIdOpt: Option[AbsoluteIri], projectionIdOpt: Option[AbsoluteIri])(
      implicit project: Project
  ): F[Option[Unit]] = {
    val msgF = IO.fromFuture(IO(ref ? RestartProjection(project.uuid, viewId, sourceIdOpt, projectionIdOpt))).to[F]
    parseOpt[Ack](msgF).recoverWith(logAndRaiseError(project.show, "restart")).map(_.map(_ => ()))
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

  /**
    * Notifies the underlying coordinator actor about an ACL change on the passed ''project''
    *
    * @param acls    the current ACLs
    * @param project the project affected by the ACLs change
    */
  def changeAcls(acls: AccessControlLists, project: Project): F[Unit] =
    for {
      views        <- cache.view.getBy[CompositeView](project.ref)
      projectsAcls <- resolveProjects(acls)
    } yield ref ! AclChanges(project.uuid, projectsAcls, views)

  private def parseOpt[A](msgF: F[Any])(implicit A: ClassTag[A], project: Project): F[Option[A]] =
    msgF.flatMap[Option[A]] {
      case Some(A(value)) => F.pure(Some(value))
      case None           => F.pure(None)
      case other =>
        val msg =
          s"Received unexpected reply from the project view coordinator actor: '$other' for project '${project.show}'."
        F.raiseError(KgError.InternalError(msg))
    }

  private def parseSet[A](msgF: F[Any])(implicit SetA: ClassTag[Set[A]], project: Project): F[Set[A]] =
    msgF.flatMap[Set[A]] {
      case SetA(value) => F.pure(value)
      case other =>
        val msg =
          s"Received unexpected reply from the project view coordinator actor: '$other' for project '${project.show}'."
        F.raiseError(KgError.InternalError(msg))
    }

  private def fetchAllStatistics(viewId: AbsoluteIri)(implicit project: Project): F[IdStatsSet] = {
    val msgF = IO.fromFuture(IO(ref ? FetchStatistics(project.uuid, viewId))).to[F]
    parseSet[IdentifiedProgress[Statistics]](msgF).recoverWith(logAndRaiseError(project.show, "statistics"))
  }

  private def fetchAllOffsets(
      viewId: AbsoluteIri
  )(implicit project: Project): F[IdOffsetSet] = {
    val msgF = IO.fromFuture(IO(ref ? FetchOffset(project.uuid, viewId))).to[F]
    parseSet[IdentifiedProgress[Offset]](msgF).recoverWith(logAndRaiseError(project.show, "progress"))
  }

  private def emptyToNone[A](set: Set[A]): Option[Set[A]] =
    if (set.isEmpty) None
    else Some(set)

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
      aclsCache: AclsCache[Task],
      as: ActorSystem,
      clients: Clients[Task],
      P: Projections[Task, String]
  ): ProjectViewCoordinator[Task] = {
    implicit val projectCache: ProjectCache[Task] = cache.project
    val coordinatorRef                            = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    new ProjectViewCoordinator[Task](cache, coordinatorRef)
  }
}
