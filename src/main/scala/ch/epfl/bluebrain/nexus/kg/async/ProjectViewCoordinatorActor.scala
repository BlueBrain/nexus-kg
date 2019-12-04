package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import akka.stream.scaladsl.Source
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor._
import ch.epfl.bluebrain.nexus.kg.cache.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressFlow.ProgressFlowElem
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.{NoProgress, SingleProgress}
import ch.epfl.bluebrain.nexus.sourcing.projections._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import shapeless.{TypeCase, Typeable}

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.concurrent.Future

/**
  * Coordinator backed by akka actor which runs the views' streams inside the provided project
  */
//noinspection ActorMutableStateInspection
private abstract class ProjectViewCoordinatorActor(viewCache: ViewCache[Task])(
    implicit val config: AppConfig,
    as: ActorSystem,
    projections: Projections[Task, String]
) extends Actor
    with Stash
    with ActorLogging {

  protected val children = mutable.Map.empty[IndexedView, ViewCoordinator]

  private var projectStream: Option[ViewCoordinator] = None

  def receive: Receive = {
    case Start(_, project: Project, views) =>
      log.debug("Started coordinator for project '{}' with initial views '{}'", project.show, views)
      context.become(initialized(project))
      viewCache.subscribe(project.ref, onChange)
      children ++= views.map(view => view -> startCoordinator(view, project, restartOffset = false))
      projectStream = Some(startProjectStream(project))
      unstashAll()
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  private def progress(viewId: AbsoluteIri, projectionId: AbsoluteIri): Task[Option[SingleProgress]] =
    children.findBy[CompositeView](viewId).flatMap {
      case (v, coord) => v.projectionView(projectionId).map((v, coord, _))
    } match {
      case Some((_, coord, pView)) => projectionProgress(coord).map(_.progress(pView.progressId)).map(Some(_))
      case _                       => Task.pure(None)
    }

  private def progress(viewId: AbsoluteIri): Task[Option[SingleProgress]] =
    children.findBy[IndexedView](viewId) match {
      case Some((v, coord)) => projectionProgress(coord).map(_.progress(v.progressId)).map(Some(_))
      case _                => Task.pure(None)
    }

  private def statistics(viewId: AbsoluteIri, projectionIdOpt: Option[AbsoluteIri] = None): Task[Option[Statistics]] = {
    val projectProgress = projectStream.map(projectionProgress).getOrElse(Task.pure(NoProgress)).map(_.minProgress)
    for {
      vp <- projectionIdOpt.map(progress(viewId, _)).getOrElse(progress(viewId))
      pp <- projectProgress.map(Some.apply)
    } yield (vp, pp) match {
      case (Some(vp), Some(pp)) =>
        Some(Statistics(vp.processed, vp.discarded, vp.failed, pp.processed, vp.offset.asInstant, pp.offset.asInstant))
      case _ => None
    }
  }

  private def projectionProgress(coordinator: ViewCoordinator): Task[ProjectionProgress] =
    coordinator.value.state().map(_.getOrElse(NoProgress))

  private def startProjectStream(project: Project): ViewCoordinator = {

    implicit val indexing: IndexingConfig = config.elasticSearch.indexing
    val name: String                      = s"project-event-count-${project.uuid}"

    val sourceF: Task[Source[ProjectionProgress, _]] = projections.progress(name).map { initial =>
      val flow = ProgressFlowElem[Task, Any]
        .collectCast[Event]
        .groupedWithin(indexing.batch, indexing.batchTimeout)
        .distinct()
        .mergeEmit()
        .toProgress(initial)
      cassandraSource(s"project=${project.uuid}", name, initial.minProgress.offset).via(flow)
    }
    ViewCoordinator(StreamSupervisor.start(sourceF, name, context.actorOf))
  }

  /**
    * Triggered in order to build an indexer actor for a provided view
    *
    * @param view          the view from where to create the indexer actor
    * @param project       the project of the current coordinator
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    * @return the actor reference
    */
  def startCoordinator(
      view: IndexedView,
      project: Project,
      restartOffset: Boolean
  ): ViewCoordinator

  /**
    * Triggered in order to build an indexer actor for a provided composite view with ability to reset the projection offset to NoOffset
    *
    * @param view               the view from where to create the indexer actor
    * @param project            the project of the current coordinator
    * @param restartOffsetViews the set of projection views for which the offset is restarted
    * @return the actor reference
    */
  def startCoordinator(
      view: CompositeView,
      project: Project,
      restartOffsetViews: Set[SingleView]
  ): ViewCoordinator

  /**
    * Triggered once an indexer actor has been stopped to clean up the indices
    *
    * @param view    the view linked to the indexer actor
    * @param project the project of the current coordinator
    */
  def deleteViewIndices(view: IndexedView, project: Project): Task[Unit]

  /**
    * Triggered when a change to key value store occurs.
    */
  def onChange: OnKeyValueStoreChange[AbsoluteIri, View]

  def initialized(project: Project): Receive = {

    // format: off
    def logStop(view: View, reason: String): Unit =
      log.info("View '{}' is going to be stopped at revision '{}' for project '{}'. Reason: '{}'.", view.id, view.rev, project.show, reason)

    def logStart(view: View, extra: String): Unit =
      log.info("View '{}' is going to be started at revision '{}' for project '{}'. {}.", view.id, view.rev, project.show, extra)
    // format: on

    def stopView(
        v: IndexedView,
        coordinator: ViewCoordinator,
        deleteIndices: Boolean = true
    ): Future[Unit] = {
      children -= v
      (coordinator.value.stop() >>
        Task.delay(coordinator.scheduleOpt.foreach(_.cancel())) >>
        (if (deleteIndices) deleteViewIndices(v, project) else Task.unit)).runToFuture
    }

    def startView(view: IndexedView, restartOffset: Boolean): Unit = {
      logStart(view, s"restartOffset: '$restartOffset'")
      children += view -> startCoordinator(view, project, restartOffset)
    }

    def startProjectionsView(view: CompositeView, restartOffsetViews: Set[SingleView]): Unit = {
      logStart(view, s"restartOffset for projections progress: '${restartOffsetViews.map(_.progressId)}'")
      children += view -> startCoordinator(view, project, restartOffsetViews)
    }

    {
      case ViewsAddedOrModified(_, restartOffset, views) =>
        val _ = views.map {
          case view if !children.keySet.exists(_.id == view.id) => startView(view, restartOffset)
          case view =>
            children
              .collectFirst { case (v, coordinator) if v.id == view.id && v.rev != view.rev => v -> coordinator }
              .foreach {
                case (old, coordinator) =>
                  startView(view, restartOffset)
                  logStop(old, s"a new rev of the same view is going to be started, restartOffset '$restartOffset'")
                  stopView(old, coordinator)
              }
        }

      case ViewsRemoved(_, views) =>
        children.view.filterKeys(v => views.exists(_.id == v.id)).foreach {
          case (v, coordinator) =>
            logStop(v, "removed from the cache")
            stopView(v, coordinator)
        }

      case ProjectChanges(_, newProject) =>
        context.become(initialized(newProject))
        children.foreach {
          case (view, coord) =>
            logStop(view, "project changed")
            stopView(view, coord).map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
        }

      case RestartView(uuid, viewId) =>
        val _ = children.findBy[IndexedView](viewId) match {
          case Some((view, coordinator)) =>
            sender() ! Option(Ack(uuid))
            logStop(view, "restart triggered from client")
            stopView(view, coordinator, deleteIndices = false)
              .map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
          case None => sender() ! None
        }

      case RestartProjections(uuid, viewId) =>
        val _ = children.findBy[CompositeView](viewId) match {
          case Some((view, coordinator)) =>
            sender() ! Option(Ack(uuid))
            logStop(view, "restart triggered from client")
            stopView(view, coordinator, deleteIndices = false)
              .map(_ => startProjectionsView(view, view.projections.map(_.view)))
          case None => sender() ! None
        }

      case RestartProjection(uuid, viewId, projectionId) =>
        val _ = children.findBy[CompositeView](viewId).flatMap {
          case (v, coordinator) => v.projectionView(projectionId).map((v, coordinator, _))
        } match {
          case Some((view, coordinator, viewProjection)) =>
            sender() ! Option(Ack(uuid))
            logStop(view, "restart triggered from client")
            stopView(view, coordinator, deleteIndices = false).map(_ => startProjectionsView(view, Set(viewProjection)))
          case None => sender() ! None
        }

      case Stop(_) =>
        children.foreach {
          case (view, coordinator) =>
            logStop(view, "deprecated organization")
            stopView(view, coordinator, deleteIndices = false)
        }

      case FetchOffset(_, viewId) =>
        val _ = progress(viewId).map(_.map(_.offset)).runToFuture pipeTo sender()

      case FetchStatistics(_, viewId) =>
        val _ = statistics(viewId).runToFuture pipeTo sender()

      case FetchProjectionStatistics(_, viewId, projectionId) =>
        val _ = statistics(viewId, Some(projectionId)).runToFuture pipeTo sender()

      case FetchProjectionOffset(_, viewId, projectionId) =>
        val _ = progress(viewId, projectionId).map(_.map(_.offset)).runToFuture pipeTo sender()

      case _: Start => //ignore, it has already been started

      case other => log.error("Unexpected message received '{}'", other)

    }

  }

}

object ProjectViewCoordinatorActor {

  final case class ViewCoordinator(
      value: StreamSupervisor[Task, ProjectionProgress],
      scheduleOpt: Option[Cancellable] = None
  )

  private[async] implicit class IndexedViewSyntax[B](private val map: mutable.Map[IndexedView, B]) extends AnyVal {
    def findBy[T <: IndexedView: Typeable](id: AbsoluteIri): Option[(T, B)] = {
      val tpe = TypeCase[T]
      map.collectFirst { case (tpe(view), value) if view.id == id => view -> value }
    }
  }

  private[async] sealed trait Msg {

    /**
      * @return the project unique identifier
      */
    def uuid: UUID
  }
  object Msg {

    final case class Start(uuid: UUID, project: Project, views: Set[IndexedView])                          extends Msg
    final case class Stop(uuid: UUID)                                                                      extends Msg
    final case class ViewsAddedOrModified(uuid: UUID, restartOffset: Boolean, views: Set[IndexedView])     extends Msg
    final case class RestartView(uuid: UUID, viewId: AbsoluteIri)                                          extends Msg
    final case class RestartProjection(uuid: UUID, viewId: AbsoluteIri, projectionsId: AbsoluteIri)        extends Msg
    final case class RestartProjections(uuid: UUID, viewId: AbsoluteIri)                                   extends Msg
    final case class Ack(uuid: UUID)                                                                       extends Msg
    final case class ViewsRemoved(uuid: UUID, views: Set[IndexedView])                                     extends Msg
    final case class ProjectChanges(uuid: UUID, project: Project)                                          extends Msg
    final case class FetchOffset(uuid: UUID, viewId: AbsoluteIri)                                          extends Msg
    final case class FetchProjectionOffset(uuid: UUID, viewId: AbsoluteIri, projectionId: AbsoluteIri)     extends Msg
    final case class FetchStatistics(uuid: UUID, viewId: AbsoluteIri)                                      extends Msg
    final case class FetchProjectionStatistics(uuid: UUID, viewId: AbsoluteIri, projectionId: AbsoluteIri) extends Msg
  }

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case msg: Msg                    => (math.abs(msg.uuid.hashCode) % shards).toString
    case ShardRegion.StartEntity(id) => (id.hashCode                 % shards).toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case msg: Msg => (msg.uuid.toString, msg)
  }

  /**
    * Starts the ProjectViewCoordinator shard that coordinates the running views' streams inside the provided project
    *
    * @param resources        the resources operations
    * @param viewCache        the view Cache
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    */
  final def start(
      resources: Resources[Task],
      viewCache: ViewCache[Task],
      shardingSettings: Option[ClusterShardingSettings],
      shards: Int
  )(
      implicit clients: Clients[Task],
      config: AppConfig,
      as: ActorSystem,
      projections: Projections[Task, String]
  ): ActorRef = {

    val props = Props(
      new ProjectViewCoordinatorActor(viewCache) {

        private def scheduleRestart(view: CompositeView): Option[Cancellable] =
          view.rebuildStrategy.map { interval =>
            as.scheduler.scheduleOnce(interval.value) {
              self ! RestartProjections(view.ref.id, view.id)
            }
          }

        private implicit val actorInitializer: (Props, String) => ActorRef = context.actorOf
        override def startCoordinator(
            view: IndexedView,
            project: Project,
            restartOffset: Boolean
        ): ViewCoordinator =
          view match {
            case v: ElasticSearchView =>
              ViewCoordinator(ElasticSearchIndexer.start(v, resources, project, restartOffset))
            case v: SparqlView =>
              ViewCoordinator(SparqlIndexer.start(v, resources, project, restartOffset))
            case v: CompositeView =>
              val cancellableOpt = scheduleRestart(v)
              val coordinator    = CompositeIndexer.start(v, resources, project, restartOffset)
              ViewCoordinator(coordinator, cancellableOpt)
          }

        override def startCoordinator(
            view: CompositeView,
            project: Project,
            restartOffsetViews: Set[SingleView]
        ): ViewCoordinator = {
          val cancellableOpt = scheduleRestart(view)
          val coordinator    = CompositeIndexer.start(view, resources, project, restartOffsetViews)
          ViewCoordinator(coordinator, cancellableOpt)
        }

        override def deleteViewIndices(view: IndexedView, project: Project): Task[Unit] = {
          def delete(v: SingleView): Task[Unit] = {
            log.info("Index '{}' is removed from project '{}'", v.index, project.show)
            v.deleteIndex >> Task.unit
          }

          view match {
            case v: SingleView =>
              delete(v)
            case v: CompositeView =>
              delete(v.defaultSparqlView) >> v.projections.map(p => delete(p.view)).toList.sequence >> Task.unit
          }
        }

        override def onChange: OnKeyValueStoreChange[AbsoluteIri, View] = onViewChange(self)

      }
    )

    start(props, shardingSettings, shards)
  }

  private[async] final def start(props: Props, shardingSettings: Option[ClusterShardingSettings], shards: Int)(
      implicit as: ActorSystem
  ): ActorRef = {

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as).start("project-view-coordinator", props, settings, entityExtractor, shardExtractor(shards))
  }

  private[async] def onViewChange(actorRef: ActorRef): OnKeyValueStoreChange[AbsoluteIri, View] =
    new OnKeyValueStoreChange[AbsoluteIri, View] {
      private val `SingleView` = TypeCase[IndexedView]

      override def apply(onChange: KeyValueStoreChanges[AbsoluteIri, View]): Unit = {
        val (toWrite, toRemove) =
          onChange.values.foldLeft((Set.empty[IndexedView], Set.empty[IndexedView])) {
            case ((write, removed), ValueAdded(_, `SingleView`(view)))    => (write + view, removed)
            case ((write, removed), ValueModified(_, `SingleView`(view))) => (write + view, removed)
            case ((write, removed), ValueRemoved(_, `SingleView`(view)))  => (write, removed + view)
            case ((write, removed), _)                                    => (write, removed)
          }
        toWrite.headOption.foreach(view => actorRef ! ViewsAddedOrModified(view.ref.id, restartOffset = false, toWrite))
        toRemove.headOption.foreach(view => actorRef ! ViewsRemoved(view.ref.id, toRemove))
      }
    }
}
