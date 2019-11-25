package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import akka.stream.scaladsl.Source
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.OffsetSyntax
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
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.NoProgress
import ch.epfl.bluebrain.nexus.sourcing.projections._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import shapeless.TypeCase

import scala.collection.immutable.Set
import scala.collection.mutable

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
    case FetchProgress(_, view) => val _ = viewProgress(view).runToFuture pipeTo sender()
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  private def viewProgress(view: SingleView): Task[Statistics] = {
    val viewProgress =
      children.get(view).map(projectionProgress).getOrElse(Task.pure(NoProgress)).map(_.progress(view.progressId))
    val projectProgress =
      projectStream.map(projectionProgress).getOrElse(Task.pure(NoProgress)).map(_.minProgress)
    for {
      vp <- viewProgress
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
    def stopView(
        v: IndexedView,
        coordinator: StreamSupervisor[Task, ProjectionProgress],
        deleteIndices: Boolean = true
    ) = {
      children -= v
      (coordinator.stop() >> (if (deleteIndices) deleteViewIndices(v, project) else Task.unit)).runToFuture
    }

    def startView(view: IndexedView, restartOffset: Boolean) = {
      log.info(
        "View '{}' is going to be started at revision '{}' for project '{}'. restartOffset: {}",
        view.id,
        view.rev,
        project.show,
        restartOffset
      )
      val ref = startCoordinator(view, project, restartOffset)
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
                case (oldView, ref) =>
                  startView(view, restartOffset)
                  log.info(
                    "View '{}' is going to be stopped at revision '{}' for project '{}' because another view is going to be started. restartOffset: {}",
                    oldView.id,
                    oldView.rev,
                    project.show,
                    restartOffset
                  )
                  stopView(oldView, ref)
              }
          case _ =>
        }

      case ViewsRemoved(_, views) =>
        children.filterKeys(v => views.exists(_.id == v.id)).foreach {
          case (v, ref) =>
            log.info(
              "View '{}' is going to be stopped at revision '{}' for project '{}' because it was removed from the cache.",
              v.id,
              v.rev,
              project.show
            )
            stopView(v, ref)
        }

      case ProjectChanges(_, newProject) =>
        context.become(initialized(newProject))
        children.foreach {
          case (view, ref) =>
            log.info(
              "View '{}' is going to be stopped at revision '{}' for project '{}' because the project has changes that require restart of the view.",
              view.id,
              view.rev,
              project.show
            )
            stopView(view, ref).map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
        }

      case Stop(_) =>
        children.foreach {
          case (view, ref) =>
            log.info(
              "View '{}' is going to be stopped at revision '{}' for project '{}' because the project or organization has been deprecated.",
              view.id,
              view.rev,
              project.show
            )
            stopView(view, ref, deleteIndices = false)
        }

      case FetchProgress(_, view) => val _ = viewProgress(view).runToFuture pipeTo sender()

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

    final case class Start(uuid: UUID, project: Project, views: Set[IndexedView])                      extends Msg
    final case class Stop(uuid: UUID)                                                                  extends Msg
    final case class ViewsAddedOrModified(uuid: UUID, restartOffset: Boolean, views: Set[IndexedView]) extends Msg
    final case class ViewsRemoved(uuid: UUID, views: Set[IndexedView])                                 extends Msg
    final case class ProjectChanges(uuid: UUID, project: Project)                                      extends Msg
    final case class FetchProgress(uuid: UUID, view: SingleView)                                       extends Msg
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

        override def deleteViewIndices(view: IndexedView, project: Project): Task[Unit] = {
          def deleteIndex(v: SingleView): Task[Unit] = {
            log.info("index '{}' is removed from project '{}'", v.index, project.show)
            v.deleteIndex >> Task.unit
          }

          view match {
            case v: SingleView =>
              deleteIndex(v)
            case v: CompositeView =>
              deleteIndex(v.defaultSparqlView) >>
                v.projections.map(p => deleteIndex(p.view)).toList.sequence >>
                Task.unit
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

  private[async] implicit class OffsetSyntax(offset: Offset) {

    val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0X01B21DD213814000L

    def asInstant: Option[Instant] = offset match {
      case NoOffset | Sequence(_) => None
      case TimeBasedUUID(uuid)    =>
        //adapted from https://support.datastax.com/hc/en-us/articles/204226019-Converting-TimeUUID-Strings-to-Dates
        Some(Instant.ofEpochMilli((uuid.timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000))
    }
  }
}
