package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import cats.MonadError
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.async.ProjectAttributesCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ProjectAttributesCoordinatorActor.OffsetSyntax
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{FileDigestNotComputed, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Files, Resource}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.FetchAttributes
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.NoProgress
import ch.epfl.bluebrain.nexus.sourcing.projections._
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicLong

/**
  * Coordinator backed by akka actor which runs the attributes stream inside the provided project
  */
//noinspection ActorMutableStateInspection
private abstract class ProjectAttributesCoordinatorActor(implicit val config: AppConfig)
    extends Actor
    with ActorLogging {

  private var child: Option[StreamSupervisor[Task, ProjectionProgress]] = None

  def receive: Receive = {
    case Start(_, project: Project) =>
      log.debug("Started attributes coordinator for project '{}'", project.projectLabel.show)
      context.become(initialized(project))
      child = Some(startCoordinator(project, restartOffset = false))
    case FetchProgress(_) => val _ = progress.runToFuture pipeTo sender()
    case other =>
      log.debug("Received non Start message '{}', ignore", other)
  }

  def initialized(project: Project): Receive = {
    case Stop(_) =>
      log.info("Attributes process for project '{}' received a stop message.", project.projectLabel.show)
      child.foreach(_.stop())
      child = None
      context.become(receive)

    case FetchProgress(_) => val _ = progress.runToFuture pipeTo sender()

    case _: Start => //ignore, it has already been started

    case other => log.error("Unexpected message received '{}'", other)
  }

  def startCoordinator(project: Project, restartOffset: Boolean): StreamSupervisor[Task, ProjectionProgress]

  private def progress: Task[Statistics] =
    child.map(_.state().map(_.getOrElse(NoProgress))).getOrElse(Task.pure(NoProgress)).map { p =>
      Statistics(p.processedCount, p.discardedCount, p.processedCount, p.offset.asInstant, p.offset.asInstant)
    }
}

private class FileDigestEventMapping[F[_]: FetchAttributes](files: Files[F])(implicit F: MonadError[F, KgError]) {

  /**
    * When an event is received, a file attributes is attempted to be calculated if the file does not currently have a digest.
    *
    * @param event event to be mapped to a Elastic Search insert query
    */
  final def apply(event: Event): F[Option[Resource]] = {
    implicit val subject = event.subject
    files.updateFileAttrEmpty(event.id).value.flatMap[Option[Resource]] {
      case Left(FileDigestNotComputed(_)) =>
        F.raiseError(InternalError(s"Resource '${event.id.ref.show}' does not have a computed digest."): KgError)
      case Left(UnexpectedState(_)) =>
        F.raiseError(InternalError(s"Storage for resource '${event.id.ref.show}' is not still on the cache."): KgError)
      case Left(_) =>
        F.pure(None)
      case Right(resource) =>
        F.pure(Some(resource))
    }
  }
}

object ProjectAttributesCoordinatorActor {

  private[async] sealed trait Msg {

    /**
      * @return the project unique identifier
      */
    def uuid: UUID
  }
  object Msg {

    final case class Start(uuid: UUID, project: Project) extends Msg
    final case class Stop(uuid: UUID)                    extends Msg
    final case class FetchProgress(uuid: UUID)           extends Msg
  }

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case msg: Msg                    => math.abs(msg.uuid.hashCode) % shards toString
    case ShardRegion.StartEntity(id) => (id.hashCode                % shards) toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case msg: Msg => (msg.uuid.toString, msg)
  }

  /**
    * Starts the ProjectDigestCoordinator shard that coordinates the running digest' streams inside the provided project
    *
    * @param files            the files operations
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    */
  final def start(
      files: Files[Task],
      shardingSettings: Option[ClusterShardingSettings],
      shards: Int
  )(
      implicit
      config: AppConfig,
      fetchDigest: FetchAttributes[Task],
      as: ActorSystem,
      projections: Projections[Task, Event]
  ): ActorRef = {
    val props = Props(new ProjectAttributesCoordinatorActor {
      override def startCoordinator(
          project: Project,
          restartOffset: Boolean
      ): StreamSupervisor[Task, ProjectionProgress] = {
        val kgErrorMonadError = ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError[Task]

        val mapper = new FileDigestEventMapping(files)(fetchDigest, kgErrorMonadError)

        val ignoreIndex: List[Resource] => Task[Unit] = _ => Task.unit

        val processedEventsGauge = Kamon
          .gauge("digest_computation_gauge")
          .withTag("type", "digest")
          .withTag("project", project.projectLabel.show)
          .withTag("organization", project.organizationLabel)
        val processedEventsCounter = Kamon
          .counter("digest_computation_counter")
          .withTag("type", "digest")
          .withTag("project", project.projectLabel.show)
          .withTag("organization", project.organizationLabel)
        val processedEventsCount = AtomicLong(0L)

        TagProjection.start(
          ProjectionConfig
            .builder[Task]
            .name(s"attributes-computation-${project.uuid}")
            .tag(s"project=${project.uuid}")
            .actorOf(context.actorOf)
            .plugin(config.persistence.queryJournalPlugin)
            .retry[KgError](config.storage.fileAttrRetry.retryStrategy)(kgErrorMonadError)
            .restart(restartOffset)
            .mapping(mapper.apply)
            .index(ignoreIndex)
            .mapInitialProgress { p =>
              processedEventsCount.set(p.processedCount)
              processedEventsGauge.update(p.processedCount.toDouble)
              Task.unit
            }
            .mapProgress { p =>
              val previousCount = processedEventsCount.get()
              processedEventsGauge.update(p.processedCount.toDouble)
              processedEventsCounter.increment(p.processedCount - previousCount)
              processedEventsCount.set(p.processedCount)
              Task.unit
            }
            .build
        )
      }
    })
    start(props, shardingSettings, shards)
  }

  private[async] final def start(props: Props, shardingSettings: Option[ClusterShardingSettings], shards: Int)(
      implicit as: ActorSystem
  ): ActorRef = {
    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as).start(
      "project-attributes-coordinator",
      props,
      settings,
      entityExtractor,
      shardExtractor(shards)
    )
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
