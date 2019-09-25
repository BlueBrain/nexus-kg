package ch.epfl.bluebrain.nexus.kg.archives

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId, Passivate}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import cats.data.OptionT
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError.{InternalError, OperationTimedOut}
import ch.epfl.bluebrain.nexus.kg.archives.ArchiveCache.Request._
import ch.epfl.bluebrain.nexus.kg.archives.ArchiveCache.{ReadResponse, WriteResponse}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import journal.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class ArchiveCache[F[_]](ref: ActorRef)(
    implicit config: ArchivesConfig,
    F: Effect[F],
    ec: ExecutionContext
) {

  private[this] val logger          = Logger[this.type]
  private implicit val tm           = Timeout(config.cacheAskTimeout)
  private implicit val contextShift = IO.contextShift(ec)

  /**
    * Retrieves the [[Archive]] from the cache
    *
    * @param resId the resource id for which imports are looked up
    * @return Some(archive) when found on the cache, None otherwise wrapped on the F[_] type
    */
  def get(resId: ResId): OptionT[F, Archive] = {

    val future = IO(ref ? Read(resId))
    val fa     = IO.fromFuture(future).to[F]
    OptionT(
      fa.flatMap[Option[Archive]] {
          case ReadResponse(archive) => F.pure(archive)
          case other =>
            val msg =
              s"Received unexpected reply from the archive cache: '$other' while fetching resource '${resId.show}'"
            F.raiseError(InternalError(msg))
        }
        .recoverWith(recovery[Option[Archive]](s"fetching resource '${resId.show}'"))
    )
  }

  def put(value: Archive): OptionT[F, Archive] = {
    val future = IO(ref ? Write(value))
    val fa     = IO.fromFuture(future).to[F]
    OptionT(
      fa.flatMap[Option[Archive]] {
          case r: WriteResponse if r.success => F.pure(Some(value))
          case _: WriteResponse              => F.pure(None)
          case other =>
            val msg =
              s"Received unexpected reply from the archive cache: '$other' while writing resource '${value.resId.show}'"
            F.raiseError(InternalError(msg))
        }
        .recoverWith(recovery[Option[Archive]](s"writing resource '${value.resId.show}'"))
    )
  }

  private def recovery[A](action: String): PartialFunction[Throwable, F[A]] = {
    case _: AskTimeoutException =>
      F.raiseError(OperationTimedOut(s"Timeout while $action from the archive cache"))
    case NonFatal(th) =>
      val msg = "Exception caught while exchanging messages with the archive cache"
      logger.error(msg, th)
      F.raiseError(InternalError(msg))
  }

}

object ArchiveCache {

  def apply[F[_]: Effect](
      shardingSettings: Option[ClusterShardingSettings] = None
  )(implicit as: ActorSystem, config: AppConfig): ArchiveCache[F] = {

    implicit val ec = as.dispatcher

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as))
    val shardExtractor: ExtractShardId = {
      case msg: Read  => math.abs(msg.resId.show.hashCode)        % config.cluster.shards toString
      case msg: Write => math.abs(msg.bundle.resId.show.hashCode) % config.cluster.shards toString
    }
    val entityExtractor: ExtractEntityId = {
      case msg: Read  => (msg.resId.show, msg)
      case msg: Write => (msg.bundle.resId.show, msg)
    }

    val props = Props(new ArchiveCoordinatorActor)

    val ref = ClusterSharding(as).start("archive-cache", props, settings, entityExtractor, shardExtractor)
    new ArchiveCache[F](ref)
  }
  private[archives] sealed trait Request extends Product with Serializable

  private[archives] object Request {
    final case class Read(resId: ResId)     extends Request
    final case class Write(bundle: Archive) extends Request
  }

  private[archives] final case object Stop
  private[archives] final case class ReadResponse(bundle: Option[Archive])
  private[archives] final case class WriteResponse(success: Boolean)

  private[archives] class ArchiveCoordinatorActor(
      implicit config: ArchivesConfig,
      ec: ExecutionContext
  ) extends Actor {
    var bundle: Option[Archive] = None

    override def preStart(): Unit = {
      val _ = context.system.scheduler.scheduleOnce(config.cacheInvalidateAfter, self, Stop)
      super.preStart()
    }

    override def receive: Receive = {
      case _: Write if bundle.isDefined =>
        sender() ! WriteResponse(success = false)

      case Write(b) =>
        bundle = Some(b)
        sender() ! WriteResponse(success = true)

      case _: Read =>
        sender() ! ReadResponse(bundle)

      case Stop =>
        context.parent ! Passivate(PoisonPill)

    }
  }
}
