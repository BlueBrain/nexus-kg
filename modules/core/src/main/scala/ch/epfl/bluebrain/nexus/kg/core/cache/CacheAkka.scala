package ch.epfl.bluebrain.nexus.kg.core.cache

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheActor.Protocol._
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheActor._
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheError._
import shapeless.Typeable

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * A cache implementation backed by akka actors shared across a cluster.
  *
  * @param ref    the underlying Actor to where perform the calls
  * @param logger the logger
  * @tparam Value the generic type of the values stored on the Cache
  */
class CacheAkka[Value](ref: ActorRef, logger: LoggingAdapter)(implicit ec: ExecutionContext, tm: Timeout)
    extends Cache[Future, Value] {

  def put(k: String, v: Value): Future[Unit] =
    if (k.isEmpty) Future.failed(EmptyKey)
    else
      ref ? Put(k, v) recoverWith recover(k, "put") flatMap {
        case true      => Future.successful(())
        case TypeError => Future.failed(TypeError)
        case other =>
          logger.error("Unexpected reply '{}' from the underlying actor while caching a key '{}' with the value '{}'",
                       other,
                       k,
                       v)
          Future.failed(UnexpectedReply(other))
      }

  def get(k: String): Future[Option[Value]] =
    if (k.isEmpty) Future.failed(EmptyKey)
    else
      ref ? Get(k) recoverWith recover(k, "get") flatMap {
        case (valueOpt: Option[Value]) => Future.successful(valueOpt)
        case other =>
          logger.error("Unexpected reply '{}' from the underlying actor while getting the value for the key '{}'",
                       other,
                       k)
          Future.failed(UnexpectedReply(other))
      }

  def remove(k: String): Future[Unit] =
    if (k.isEmpty) Future.failed(EmptyKey)
    else
      ref ? Remove(k) recoverWith recover(k, "remove") flatMap {
        case true => Future.successful(())
        case other =>
          logger.error("Unexpected reply '{}' from the underlying actor while removing the value of a key '{}'",
                       other,
                       k)
          Future.failed(UnexpectedReply(other))
      }

  private def recover(k: String, operation: String): PartialFunction[Throwable, Future[Any]] = {
    // $COVERAGE-OFF$
    case _: AskTimeoutException =>
      logger.error("Timed out while performing operation '{}' on the key '{}'", operation, k)
      Future.failed(TimeoutError)
    case NonFatal(th) =>
      logger.error(th, "Unexpected exception while performing operation '{}' on the key '{}'", k, operation)
      Future.failed(UnknownError(th))
    // $COVERAGE-ON$
  }

}

object CacheAkka {

  /**
    * Data type to represent general settings for the akka based cache implementation.
    *
    * @param passivationTimeout the inactivity timeout after which an actor will request to be stopped
    * @param askTimeout         the future completion timeout while interacting with a persistent actor
    * @param shards             the number of shards to create for an aggregate
    */
  final case class CacheSettings(passivationTimeout: FiniteDuration = 1 hour,
                                 askTimeout: FiniteDuration = 15 seconds,
                                 shards: Int = 100)

  private def shardExtractor(shards: Int): ExtractShardId = {
    case msg: Protocol => math.abs(msg.key.hashCode) % shards toString
  }

  private val entityExtractor: ExtractEntityId = {
    case msg: Protocol => (msg.key, msg)
  }

  /**
    * Constructs a new Cache implementation with Akka Actors shared across a cluster.
    *
    * @param name     the name of the cluster
    * @param settings the akka settings
    * @tparam Value the generic type of the values stored on the Cache
    */
  final def apply[Value: Typeable](name: String, settings: CacheSettings)(
      implicit as: ActorSystem): Cache[Future, Value] = {

    implicit val ec = as.dispatcher
    implicit val tm = Timeout(settings.askTimeout)

    val props = Props[CacheActor[Value]](new CacheActor(settings.passivationTimeout))
    val ref = ClusterSharding(as)
      .start(name, props, ClusterShardingSettings(as), entityExtractor, shardExtractor(settings.shards))

    new CacheAkka(ref, Logging(as, s"CacheAkka-$name"))
  }
}
