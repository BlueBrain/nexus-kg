package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStore.Subscription
import ch.epfl.bluebrain.nexus.service.indexer.cache.{KeyValueStore, KeyValueStoreConfig, OnKeyValueStoreChange}

abstract class Cache[F[_]: Monad, V](private[async] val store: KeyValueStore[F, UUID, V]) {

  /**
    * Adds a subscription to the cache
    *
    * @param value the method that gets triggered when a change to key value store occurs
    */
  def subscribe(value: OnKeyValueStoreChange[UUID, V]): F[KeyValueStore.Subscription] = store.subscribe(value)

  /**
    * Removes a subscription from the cache
    *
    * @param subscription the subscription to be removed
    */
  def unsubscribe(subscription: Subscription): F[Unit] = store.unsubscribe(subscription)

  private[async] def get(id: UUID): F[Option[V]] = store.get(id)

  private[async] def replace(id: UUID, value: V): F[Unit] = store.put(id, value)
}

object Cache {

  private[async] def storeWrappedError[F[_]: Timer, V](
      name: String,
      f: V => Long)(implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): KeyValueStore[F, UUID, V] =
    new KeyValueStore[F, UUID, V] {
      val underlying: KeyValueStore[F, UUID, V] = KeyValueStore.distributed(name, (_, resource) => f(resource))

      override def put(key: UUID, value: V): F[Unit] =
        underlying.put(key, value).recoverWith { case err => F.raiseError(InternalError(err.getMessage)) }

      override def entries: F[Map[UUID, V]] =
        underlying.entries.recoverWith { case err => F.raiseError(InternalError(err.getMessage)) }

      override def remove(key: UUID): F[Unit] =
        underlying.remove(key).recoverWith { case err => F.raiseError(InternalError(err.getMessage)) }

      override def subscribe(value: OnKeyValueStoreChange[UUID, V]): F[KeyValueStore.Subscription] =
        underlying.subscribe(value)

      override def unsubscribe(subscription: KeyValueStore.Subscription): F[Unit] =
        underlying.unsubscribe(subscription)
    }
}
