package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
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
import shapeless.TypeCase

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

  private val children = mutable.Map.empty[IndexedView, StreamSupervisor[Task, ProjectionProgress]]

  private var projectStream: Option[StreamSupervisor[Task, ProjectionProgress]] = None

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

  private def progress(mainView: IndexedView, singleViewOpt: Option[SingleView] = None): Task[SingleProgress] = {
    val progressId = singleViewOpt.map(_.progressId).getOrElse(mainView.progressId)
    children.get(mainView).map(projectionProgress).getOrElse(Task.pure(NoProgress)).map(_.progress(progressId))
  }

  private def statistics(view: IndexedView, singleViewOpt: Option[SingleView] = None): Task[Statistics] = {
    val projectProgress =
      projectStream.map(projectionProgress).getOrElse(Task.pure(NoProgress)).map(_.minProgress)
    for {
      vp <- progress(view, singleViewOpt)
      pp <- projectProgress
    } yield Statistics(
      vp.processed,
      vp.discarded,
      vp.failed,
      pp.processed,
      vp.offset.asInstant,
      pp.offset.asInstant
    )
  }

  private def projectionProgress(coordinator: StreamSupervisor[Task, ProjectionProgress]): Task[ProjectionProgress] =
    coordinator.state().map(_.getOrElse(NoProgress))

  private def startProjectStream(project: Project): StreamSupervisor[Task, ProjectionProgress] = {

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
    StreamSupervisor.start(sourceF, name, context.actorOf)
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
  ): StreamSupervisor[Task, ProjectionProgress]

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
  ): StreamSupervisor[Task, ProjectionProgress]

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
        coordinator: StreamSupervisor[Task, ProjectionProgress],
        deleteIndices: Boolean = true
    ): Future[Unit] = {
      children -= v
      (coordinator.stop() >> (if (deleteIndices) deleteViewIndices(v, project) else Task.unit)).runToFuture
    }

    def startView(view: IndexedView, restartOffset: Boolean): Unit = {
      logStart(view, s"restartOffset: '$restartOffset'")
      val ref = startCoordinator(view, project, restartOffset)
      children += view -> ref
    }

    def startProjectionsView(view: CompositeView, restartOffsetViews: Set[SingleView]): Unit = {
      logStart(view, s"restartOffset for projections: '${restartOffsetViews.map(_.progressId)}'")
      val ref = startCoordinator(view, project, restartOffsetViews)
      children += view -> ref
    }

    {
      case ViewsAddedOrModified(_, restartOffset, views) =>
        views.foreach {
          case view if !children.keySet.exists(_.id == view.id) => startView(view, restartOffset)
          case view: IndexedView =>
            children
              .collectFirst {
                case (v: IndexedView, ref) if v.id == view.id && v.ref == view.ref && v.rev != view.rev =>
                  v -> ref
              }
              .foreach {
                case (old, ref) =>
                  startView(view, restartOffset)
                  logStop(old, s"a new rev of the same view is going to be started, restartOffset '$restartOffset'")
                  stopView(old, ref)
              }
          case _ =>
        }

      case ViewsRemoved(_, views) =>
        children.filterKeys(v => views.exists(_.id == v.id)).foreach {
          case (v, ref) =>
            logStop(v, "removed from the cache")
            stopView(v, ref)
        }

      case ProjectChanges(_, newProject) =>
        context.become(initialized(newProject))
        children.foreach {
          case (view, ref) =>
            logStop(view, "project changed")
            stopView(view, ref).map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
        }

      case RestartView(_, view) =>
        children.find { case (v, _) => v.id == view.id }.foreach {
          case (v, ref) =>
            logStop(v, "restart triggered from client")
            stopView(v, ref, deleteIndices = false)
              .map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
        }

      case RestartProjections(_, view, projections) =>
        children.collectFirst { case (v: CompositeView, ref) if v.id == view.id => view -> ref }.foreach {
          case (v, ref) =>
            logStop(v, "restart triggered from client")
            stopView(v, ref, deleteIndices = false).map(_ => startProjectionsView(view, projections))
        }

      case Stop(_) =>
        children.foreach {
          case (view, ref) =>
            logStop(view, "deprecated organization")
            stopView(view, ref, deleteIndices = false)
        }

      case FetchOffset(_, view) =>
        val _ = progress(view).map(_.offset).runToFuture pipeTo sender()

      case FetchStatistics(_, view) =>
        val _ = statistics(view).runToFuture pipeTo sender()

      case FetchProjectionStatistics(_, mainView, projection) =>
        val _ = statistics(mainView, Some(projection)).runToFuture pipeTo sender()

      case FetchProjectionOffset(_, mainView, projection) =>
        val _ = progress(mainView, Some(projection)).map(_.offset).runToFuture pipeTo sender()

      case _: Start => //ignore, it has already been started

      case other => log.error("Unexpected message received '{}'", other)

    }

  }

}

object ProjectViewCoordinatorActor {

  private[async] sealed trait Msg {

    /**
      * @return the project unique identifier
      */
    def uuid: UUID
  }
  object Msg {

    final case class Start(uuid: UUID, project: Project, views: Set[IndexedView])                           extends Msg
    final case class Stop(uuid: UUID)                                                                       extends Msg
    final case class ViewsAddedOrModified(uuid: UUID, restartOffset: Boolean, views: Set[IndexedView])      extends Msg
    final case class RestartView(uuid: UUID, view: IndexedView)                                             extends Msg
    final case class RestartProjections(uuid: UUID, view: CompositeView, projectionViews: Set[SingleView])  extends Msg
    final case class ViewsRemoved(uuid: UUID, views: Set[IndexedView])                                      extends Msg
    final case class ProjectChanges(uuid: UUID, project: Project)                                           extends Msg
    final case class FetchOffset(uuid: UUID, view: IndexedView)                                             extends Msg
    final case class FetchProjectionOffset(uuid: UUID, view: CompositeView, projectionView: SingleView)     extends Msg
    final case class FetchStatistics(uuid: UUID, view: IndexedView)                                         extends Msg
    final case class FetchProjectionStatistics(uuid: UUID, view: CompositeView, projectionView: SingleView) extends Msg
  }

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case msg: Msg                    => math.abs(msg.uuid.hashCode) % shards toString
    case ShardRegion.StartEntity(id) => (id.hashCode                % shards) toString
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

        private implicit val actorInitializer: (Props, String) => ActorRef = context.actorOf
        override def startCoordinator(
            view: IndexedView,
            project: Project,
            restartOffset: Boolean
        ): StreamSupervisor[Task, ProjectionProgress] =
          view match {
            case v: ElasticSearchView => ElasticSearchIndexer.start(v, resources, project, restartOffset)
            case v: SparqlView        => SparqlIndexer.start(v, resources, project, restartOffset)
            case v: CompositeView     => CompositeIndexer.start(v, resources, project, restartOffset)
          }

        override def startCoordinator(
            view: CompositeView,
            project: Project,
            restartOffsetViews: Set[SingleView]
        ): StreamSupervisor[Task, ProjectionProgress] =
          CompositeIndexer.start(view, resources, project, restartOffsetViews)

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
