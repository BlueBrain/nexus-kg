package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.{ask, pipe}
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{withUnmarshaller, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.OffsetSyntax
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ViewCache.RevisionedViews
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticSearchIndexer, SparqlIndexer, View}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress.NoProgress
import ch.epfl.bluebrain.nexus.sourcing.persistence.{IndexerConfig, ProjectionProgress, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator.{
  FetchLatestState,
  LatestState,
  Stop => StreamCoordinatorStop
}
import io.circe.Json
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicLong
import org.apache.jena.query.ResultSet
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

  private val children = mutable.Map.empty[SingleView, ActorRef]

  private var projectStream: Option[ActorRef] = None

  def receive: Receive = {
    case Start(_, project: Project, views) =>
      log.debug("Started coordinator for project '{}' with initial views '{}'", project.projectLabel.show, views)
      context.become(initialized(project))
      viewCache.subscribe(onChange(project.ref))
      children ++= views.map(view => view -> startActor(view, project, restartOffset = false))
      projectStream = Some(startProjectStream(project))
      unstashAll()
    case FetchProgress(_, view: SingleView) => val _ = viewProgress(view).pipeTo(sender())
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  private def viewProgress(view: SingleView): Future[ViewProgress] = {
    val viewProgress    = children.get(view).map(projectionProgress).getOrElse(Future.successful(NoProgress))
    val projectProgress = projectStream.map(projectionProgress).getOrElse(Future.successful(NoProgress))
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

  private def projectionProgress(actor: ActorRef): Future[ProjectionProgress] = {
    implicit val timeout: Timeout = config.sourcing.askTimeout
    (actor ? FetchLatestState)
      .map {
        case LatestState(Some(p: ProjectionProgress)) => p
        case LatestState(_)                           => NoProgress
      }
  }

  private def startProjectStream(project: Project): ActorRef = {
    implicit val indexing = config.indexing.elasticSearch
    import ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
    implicit val iam = config.iam.iamClient
    val g = Kamon
      .gauge("kg_indexer_gauge")
      .refine(
        "type"         -> "eventCount",
        "project"      -> s"${project.organizationLabel}/${project.label}",
        "organization" -> project.organizationLabel
      )
    val c = Kamon
      .counter("kg_indexer_counter")
      .refine(
        "type"         -> "eventCount",
        "project"      -> s"${project.organizationLabel}/${project.label}",
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
  def startActor(view: SingleView, project: Project, restartOffset: Boolean): ActorRef

  /**
    * Triggered once an indexer actor has been stopped to clean up the indices
    *
    * @param view    the view linked to the indexer actor
    * @param project the project of the current coordinator
    */
  def deleteViewIndices(view: SingleView, project: Project): Task[Unit]

  /**
    * Triggered when a change to key value store occurs.
    *
    * @param projectRef the project unique reference
    */
  def onChange(projectRef: ProjectRef): OnKeyValueStoreChange[UUID, RevisionedViews]

  private def stopActor(ref: ActorRef): Unit = {
    ref ! StreamCoordinatorStop
    context.stop(ref)

  }

  def initialized(project: Project): Receive = {
    def stopView(v: SingleView, ref: ActorRef, deleteIndices: Boolean = true) = {
      stopActor(ref)
      children -= v
      if (deleteIndices) deleteViewIndices(v, project).runToFuture else Future.unit
    }

    def startView(view: SingleView, restartOffset: Boolean) = {
      val ref = startActor(view, project, restartOffset)
      children += view -> ref
    }

    {
      case ViewsChanges(_, restartOffset, views) =>
        views.map {
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

        val toRemove = children.filterNot { case (v, _) => views.exists(_.id == v.id) }
        toRemove.foreach { case (v, ref) => stopView(v, ref) }

      case ProjectChanges(_, newProject) =>
        context.become(initialized(newProject))
        children.foreach {
          case (view, ref) =>
            stopView(view, ref).map(_ => self ! ViewsChanges(project.uuid, restartOffset = true, Set(view)))
        }

      case Stop(_) =>
        children.foreach { case (view, ref) => stopView(view, ref, deleteIndices = false) }
      case FetchProgress(_, view: SingleView) => val _ = viewProgress(view).pipeTo(sender())
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

    final case class Start(uuid: UUID, project: Project, views: Set[SingleView])              extends Msg
    final case class Stop(uuid: UUID)                                                         extends Msg
    final case class ViewsChanges(uuid: UUID, restartOffset: Boolean, views: Set[SingleView]) extends Msg
    final case class ProjectChanges(uuid: UUID, project: Project)                             extends Msg
    final case class FetchProgress(uuid: UUID, view: View)                                    extends Msg

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
                               mt: ActorMaterializer,
                               ul: UntypedHttpClient[Task],
                               ucl: HttpClient[Task, ResultSet],
                               as: ActorSystem): ActorRef = {

    val props = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        private implicit val retry: Retry[Task, Throwable] = Retry(config.indexing.keyValueStore.retry.retryStrategy)

        private val sparql                                      = config.sparql
        private implicit val jsonClient: HttpClient[Task, Json] = withUnmarshaller[Task, Json]

        override def startActor(view: SingleView, project: Project, restartOffset: Boolean): ActorRef =
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

        override def onChange(projectRef: ProjectRef): OnKeyValueStoreChange[UUID, RevisionedViews] =
          onViewChange(projectRef, self)

      }
    )

    start(props, shardingSettings, shards)
  }

  private[async] final def start(props: Props, shardingSettings: Option[ClusterShardingSettings], shards: Int)(
      implicit as: ActorSystem): ActorRef = {

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as).start("project-view-coordinator", props, settings, entityExtractor, shardExtractor(shards))
  }

  private[async] def onViewChange(projectRef: ProjectRef,
                                  actorRef: ActorRef): OnKeyValueStoreChange[UUID, RevisionedViews] =
    new OnKeyValueStoreChange[UUID, RevisionedViews] {

      private def singleViews(values: Set[View]): Set[SingleView] = values.collect { case v: SingleView => v }
      private val SetView                                         = TypeCase[RevisionedViews]
      private val projectUuid                                     = projectRef.id

      override def apply(onChange: KeyValueStoreChanges[UUID, RevisionedViews]): Unit = {
        val views = onChange.values.foldLeft(Set.empty[SingleView]) {
          case (acc, ValueAdded(`projectUuid`, SetView(revValue)))    => acc ++ singleViews(revValue.value)
          case (acc, ValueModified(`projectUuid`, SetView(revValue))) => acc ++ singleViews(revValue.value)
          case (acc, _)                                               => acc
        }
        if (views.nonEmpty) actorRef ! ViewsChanges(projectUuid, restartOffset = false, views)

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
