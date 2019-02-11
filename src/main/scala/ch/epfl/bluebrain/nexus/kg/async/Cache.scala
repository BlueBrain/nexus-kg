package ch.epfl.bluebrain.nexus.kg.async

import cats.Monad
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStore.Subscription
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreError._
import ch.epfl.bluebrain.nexus.service.indexer.cache._

abstract class Cache[F[_]: Monad, K, V](private[async] val store: KeyValueStore[F, K, V]) {

  /**
    * Adds a subscription to the cache
    *
    * @param value the method that gets triggered when a change to key value store occurs
    */
  def subscribe(value: OnKeyValueStoreChange[K, V]): F[KeyValueStore.Subscription] = store.subscribe(value)

  /**
    * Removes a subscription from the cache
    *
    * @param subscription the subscription to be removed
    */
  def unsubscribe(subscription: Subscription): F[Unit] = store.unsubscribe(subscription)

  private[async] def get(id: K): F[Option[V]] = store.get(id)

  private[async] def replace(id: K, value: V): F[Unit] = store.put(id, value)
}

object Cache {

  private[async] def mapError(cacheError: KeyValueStoreError): KgError =
    cacheError match {
      case e: ReadWriteConsistencyTimeout =>
        OperationTimedOut(s"Timeout while interacting with the cache due to '${e.timeout}'")
      case e: DistributedDataError => InternalError(e.reason)
    }
}
