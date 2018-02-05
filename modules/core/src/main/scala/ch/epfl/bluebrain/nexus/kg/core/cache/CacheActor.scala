package ch.epfl.bluebrain.nexus.kg.core.cache

import akka.actor.{Actor, ActorLogging, PoisonPill, ReceiveTimeout}
import akka.cluster.sharding.ShardRegion.Passivate
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheActor.Protocol._
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheError.TypeError
import shapeless.{Typeable, the}

import scala.concurrent.duration._

/**
  * Actor that stores in memory a ''Value''.
  *
  * @param passivationTimeout the inactivity interval after which the actor requests to be stopped
  * @tparam Value the generic type of the value stored on this actor
  */
class CacheActor[Value: Typeable](passivationTimeout: FiniteDuration) extends Actor with ActorLogging {

  private val Value = the[Typeable[Value]]

  private var value: Option[Value] = None

  override def preStart(): Unit = {
    context.setReceiveTimeout(passivationTimeout)
    super.preStart()
  }

  override def receive: Receive = {
    case Get(k) =>
      value match {
        case Some(v) =>
          log.debug("Cached key '{}' found with value  '{}'", k, v)
        case None =>
          log.debug("Cached key '{}' not found. Proceed to update pasivation timeout to 10 sec", k)
          context.parent ! Passivate(stopMessage = PoisonPill)
      }
      sender() ! value

    case Put(k, v) =>
      Value.cast(v) match {
        case Some(cache) =>
          value = Some(cache)
          sender() ! Ack(k)
          log.debug("Added to cache key '{}' with value '{}'", k, cache)
        case None =>
          log.error("Received a value '{}' incompatible with the expected type '{}'", v, Value.describe)
          sender() ! TypeError
      }

    case Remove(k) =>
      log.debug("Received a delete message for to cached key '{}'. Proceed to stop actor", k)
      value = None
      sender() ! Ack(k)
      context.stop(self)

  }

  override def unhandled(message: Any): Unit = message match {
    case ReceiveTimeout => context.parent ! Passivate(stopMessage = PoisonPill)
    case other          => super.unhandled(other)
  }
}

object CacheActor {

  /**
    * Enumeration that defines the message types with this actor.
    */
  sealed trait Protocol extends Product with Serializable {
    def key: String
  }

  object Protocol {

    /**
      * Get message. When received, the actor retrieves its in-memory value.
      *
      * @param key the key for which the in-memory value is going to be retrieved
      */
    final case class Get(key: String) extends Protocol

    /**
      * Remove message. When received, the actor removes its in-memory value.
      *
      * @param key the key for which the in-memory value is going to be removed
      */
    final case class Remove(key: String) extends Protocol

    /**
      * Put message. When received, the actor set its in-memory value to the provided ''value''.
      *
      * @param key   the key for which the provided ''value'' is going to be added
      * @param value the value to be added
      * @tparam Value the generic type of the value stored on this actor
      */
    final case class Put[Value](key: String, value: Value) extends Protocol

    /**
      * Positive return message
      * @param key the returned key
      */
    final case class Ack(key: String) extends Protocol
  }

}
