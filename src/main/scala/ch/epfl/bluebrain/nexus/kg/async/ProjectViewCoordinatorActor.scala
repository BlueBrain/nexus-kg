package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.OffsetSyntax
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticSearchIndexer, SparqlIndexer, View}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Resources}
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress.NoProgress
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, ProjectionProgress, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicLong
import shapeless.TypeCase

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.concurrent.Future

/**
  * Coordinator backed by akka actor which runs the views' streams inside the provided project
  */
private abstract class ProjectViewCoordinatorActor(viewCache: ViewCache[Task])(implicit val config: AppConfig,
                                                                               as: ActorSystem)
    extends Actor
    with Stash
    with ActorLogging {

  private val children = mutable.Map.empty[SingleView, StreamCoordinator[Task, ProjectionProgress]]

  private var projectStream: Option[StreamCoordinator[Task, ProjectionProgress]] = None

  def receive: Receive = {
    case Start(_, project: Project, views) =>
      log.debug("Started coordinator for project '{}' with initial views '{}'", project.projectLabel.show, views)
      context.become(initialized(project))
      viewCache.subscribe(project.ref, onChange)
      children ++= views.map(view => view -> startCoordinator(view, project, restartOffset = false))
      projectStream = Some(startProjectStream(project))
      unstashAll()
    case FetchProgress(_, view: SingleView) => val _ = viewProgress(view).runToFuture pipeTo sender()
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  private def viewProgress(view: SingleView): Task[ViewProgress] = {
    val viewProgress    = children.get(view).map(projectionProgress).getOrElse(Task.pure(NoProgress))
    val projectProgress = projectStream.map(projectionProgress).getOrElse(Task.pure(NoProgress))
    for {
      vp <- viewProgress
      pp <- projectProgress
    } yield
      ViewProgress(
        vp.processedCount,
        vp.discardedCount,
        pp.processedCount,
        vp.offset.asInstant,
        pp.offset.asInstant
      )
  }

  private def projectionProgress(coordinator: StreamCoordinator[Task, ProjectionProgress]): Task[ProjectionProgress] =
    coordinator.state().map(_.getOrElse(NoProgress))

  private def startProjectStream(project: Project): StreamCoordinator[Task, ProjectionProgress] = {
    implicit val indexing = config.elasticSearch.indexing
    import ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
    implicit val iam            = config.iam.iamClient
    implicit val sourcingConfig = config.sourcing
    val g = Kamon
      .gauge("kg_indexer_gauge")
      .refine(
        "type"         -> "eventCount",
        "project"      -> project.projectLabel.show,
        "organization" -> project.organizationLabel
      )
    val c = Kamon
      .counter("kg_indexer_counter")
      .refine(
        "type"         -> "eventCount",
        "project"      -> project.projectLabel.show,
        "organization" -> project.organizationLabel
      )
    val count = AtomicLong(0L)
    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name(s"project-event-count-${project.uuid}")
        .tag(s"project=${project.uuid}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[ElasticSearchServerOrUnexpectedFailure](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(false)
        .init(Task.unit)
        .mapping[Event, Event](a => Task.pure(Some(a)))
        .index(_ => Task.unit)
        .mapInitialProgress { p =>
          count.set(p.processedCount)
          g.set(p.processedCount)
          Task.unit
        }
        .mapProgress { p =>
          val previousCount = count.get()
          g.set(p.processedCount)
          c.increment(p.processedCount - previousCount)
          count.set(p.processedCount)
          Task.unit
        }
        .build)
  }

  /**
    * Triggered in order to build an indexer actor for a provided view
    *
    * @param view          the view from where to create the indexer actor
    * @param project       the project of the current coordinator
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    * @return the actor reference
    */
  def startCoordinator(view: SingleView,
                       project: Project,
                       restartOffset: Boolean): StreamCoordinator[Task, ProjectionProgress]

  /**
    * Triggered once an indexer actor has been stopped to clean up the indices
    *
    * @param view    the view linked to the indexer actor
    * @param project the project of the current coordinator
    */
  def deleteViewIndices(view: SingleView, project: Project): Task[Unit]

  /**
    * Triggered when a change to key value store occurs.
    */
  def onChange: OnKeyValueStoreChange[AbsoluteIri, View]

  def initialized(project: Project): Receive = {
    def stopView(v: SingleView,
                 coordinator: StreamCoordinator[Task, ProjectionProgress],
                 deleteIndices: Boolean = true) = {
      coordinator.stop()
      children -= v
      if (deleteIndices) deleteViewIndices(v, project).runToFuture else Future.unit
    }

    def startView(view: SingleView, restartOffset: Boolean) = {
      val ref = startCoordinator(view, project, restartOffset)
      children += view -> ref
    }

    {
      case ViewsAddedOrModified(_, restartOffset, views) =>
        views.foreach {
          case view if !children.keySet.exists(_.id == view.id) => startView(view, restartOffset)
          case view: ElasticSearchView =>
            children
              .collectFirst {
                case (v: ElasticSearchView, ref) if v.id == view.id && v.ref == view.ref && v.rev != view.rev =>
                  v -> ref
              }
              .foreach {
                case (oldView, ref) =>
                  startView(view, restartOffset)
                  stopView(oldView, ref)
              }
          case _ =>
        }

      case ViewsRemoved(_, views) =>
        children.foreach { case (v, ref) if views.exists(_.id == v.id) => stopView(v, ref)}

      case ProjectChanges(_, newProject) =>
        context.become(initialized(newProject))
        children.foreach {
          case (view, ref) =>
            stopView(view, ref).map(_ => self ! ViewsAddedOrModified(project.uuid, restartOffset = true, Set(view)))
        }

      case Stop(_) =>
        children.foreach { case (view, ref) => stopView(view, ref, deleteIndices = false) }
      case FetchProgress(_, view: SingleView) => val _ = viewProgress(view).runToFuture pipeTo sender()
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

    final case class Start(uuid: UUID, project: Project, views: Set[SingleView])                      extends Msg
    final case class Stop(uuid: UUID)                                                                 extends Msg
    final case class ViewsAddedOrModified(uuid: UUID, restartOffset: Boolean, views: Set[SingleView]) extends Msg
    final case class ViewsRemoved(uuid: UUID, views: Set[SingleView])                                 extends Msg
    final case class ProjectChanges(uuid: UUID, project: Project)                                     extends Msg
    final case class FetchProgress(uuid: UUID, view: View)                                            extends Msg

    final case class ViewProgress(processedEvents: Long,
                                  discardedEvents: Long,
                                  totalEvents: Long,
                                  lastProcessedEvent: Option[Instant],
                                  lastEvent: Option[Instant])
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
  final def start(resources: Resources[Task],
                  viewCache: ViewCache[Task],
                  shardingSettings: Option[ClusterShardingSettings],
                  shards: Int)(implicit esClient: ElasticSearchClient[Task],
                               config: AppConfig,
                               ul: UntypedHttpClient[Task],
                               ucl: HttpClient[Task, SparqlResults],
                               as: ActorSystem): ActorRef = {

    val props = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        private implicit val retry: Retry[Task, Throwable] = Retry(config.keyValueStore.indexing.retry.retryStrategy)

        private val sparql = config.sparql

        override def startCoordinator(view: SingleView,
                                      project: Project,
                                      restartOffset: Boolean): StreamCoordinator[Task, ProjectionProgress] =
          view match {
            case v: ElasticSearchView => ElasticSearchIndexer.start(v, resources, project, restartOffset)
            case v: SparqlView        => SparqlIndexer.start(v, resources, project, restartOffset)
          }

        override def deleteViewIndices(view: SingleView, project: Project): Task[Unit] = view match {
          case v: ElasticSearchView =>
            log.info("ElasticSearchView index '{}' is removed from project '{}'", v.index, project.projectLabel.show)
            esClient
              .deleteIndex(v.index)
              .mapRetry({ case true => () },
                        KgError.InternalError(s"Could not delete ElasticSearch index '${v.index}'"): Throwable)
          case _: SparqlView =>
            log.info("Blazegraph keyspace '{}' is removed from project '{}'", view.name, project.projectLabel.show)
            val client = BlazegraphClient[Task](sparql.base, view.name, sparql.akkaCredentials)
            client.deleteNamespace.mapRetry(
              { case true => () },
              KgError.InternalError(s"Could not delete Sparql keyspace '${view.name}'"): Throwable)
        }

        override def onChange: OnKeyValueStoreChange[AbsoluteIri, View] =
          onViewChange(self)

      }
    )

    start(props, shardingSettings, shards)
  }

  private[async] final def start(props: Props, shardingSettings: Option[ClusterShardingSettings], shards: Int)(
      implicit as: ActorSystem): ActorRef = {

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as).start("project-view-coordinator", props, settings, entityExtractor, shardExtractor(shards))
  }

  private[async] def onViewChange(actorRef: ActorRef): OnKeyValueStoreChange[AbsoluteIri, View] =
    new OnKeyValueStoreChange[AbsoluteIri, View] {
      private val `SingleView` = TypeCase[SingleView]

      override def apply(onChange: KeyValueStoreChanges[AbsoluteIri, View]): Unit = {
        val (toWrite, toRemove) =
          onChange.values.foldLeft((Set.empty[SingleView], Set.empty[SingleView])) {
            case ((write, removed), ValueAdded(_, `SingleView`(view)))    => (write + view, removed)
            case ((write, removed), ValueModified(_, `SingleView`(view))) => (write + view, removed)
            case ((write, removed), ValueRemoved(_, `SingleView`(view)))  => (write, removed + view)
          }
        toWrite.headOption.foreach(view => actorRef ! ViewsAddedOrModified(view.ref.id, restartOffset = false, toWrite))
        toRemove.headOption.foreach(view => actorRef ! ViewsRemoved(view.ref.id, toRemove))

      }
    }

  private[async] implicit class OffsetSyntax(offset: Offset) {

    val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

    def asInstant: Option[Instant] = offset match {
      case NoOffset | Sequence(_) => None
      case TimeBasedUUID(uuid)    =>
        //adapted from https://support.datastax.com/hc/en-us/articles/204226019-Converting-TimeUUID-Strings-to-Dates
        Some(Instant.ofEpochMilli((uuid.timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000))
    }
  }
}
