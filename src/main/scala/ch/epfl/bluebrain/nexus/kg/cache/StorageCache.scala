package ch.epfl.bluebrain.nexus.kg.cache

import java.time.{Clock, Instant}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig}
import ch.epfl.bluebrain.nexus.kg.RevisionedValue
import ch.epfl.bluebrain.nexus.kg.cache.Cache._
import ch.epfl.bluebrain.nexus.kg.cache.StorageProjectCache._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

class StorageCache[F[_]: Timer] private (projectToCache: ConcurrentHashMap[UUID, StorageProjectCache[F]])(
    implicit as: ActorSystem,
    F: Async[F],
    config: KeyValueStoreConfig,
    clock: Clock) {

  /**
    * Fetches storages for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[List[Storage]] =
    getOrCreate(ref).get

  /**
    * Fetches storage from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the view unique id in the provided project
    */
  def get(ref: ProjectRef, id: AbsoluteIri): F[Option[Storage]] =
    getOrCreate(ref).getBy(id)

  /**
    * Fetches the default storage from the provided project.
    *
    * @param ref the project unique reference
    */
  def getDefault(ref: ProjectRef): F[Option[Storage]] =
    getOrCreate(ref).getDefault

  /**
    * Adds/updates or deprecates a storage on the provided project.
    *
    * @param storage the storage value
    */
  def put(storage: Storage)(implicit instant: Instant = clock.instant()): F[Unit] =
    getOrCreate(storage.ref).put(storage)

  private def getOrCreate(ref: ProjectRef): StorageProjectCache[F] =
    projectToCache.getSafe(ref.id).getOrElse(projectToCache.putAndReturn(ref.id, StorageProjectCache[F](ref)))
}

/**
  * The storage cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
private class StorageProjectCache[F[_]] private (store: KeyValueStore[F, AbsoluteIri, RevisionedStorage])(
    implicit F: Monad[F])
    extends Cache[F, AbsoluteIri, RevisionedStorage](store) {

  private implicit val ordering: Ordering[RevisionedStorage] = Ordering.by((s: RevisionedStorage) => s.rev).reverse

  private implicit def revisioned(storage: Storage)(implicit instant: Instant): RevisionedStorage =
    RevisionedValue(instant.toEpochMilli, storage)

  def get: F[List[Storage]] =
    store.values.map(_.toList.sorted.map(_.value))

  def getDefault: F[Option[Storage]] =
    get.map(_.collectFirst { case storage if storage.default => storage })

  def getBy(id: AbsoluteIri): F[Option[Storage]] =
    get(id).map(_.collectFirst { case RevisionedValue(_, storage) if storage.id == id => storage })

  def put(storage: Storage)(implicit instant: Instant): F[Unit] =
    if (storage.deprecated) store.remove(storage.id)
    else store.put(storage.id, storage)
}

private object StorageProjectCache {

  type RevisionedStorage = RevisionedValue[Storage]

  def apply[F[_]: Timer](project: ProjectRef)(implicit as: ActorSystem,
                                              config: KeyValueStoreConfig,
                                              F: Async[F]): StorageProjectCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    new StorageProjectCache(
      KeyValueStore.distributed(s"storage-${project.id}", (_, storage) => storage.value.rev, mapError))(F)
  }

}

object StorageCache {

  def apply[F[_]: Timer: Async](implicit as: ActorSystem, config: KeyValueStoreConfig, clock: Clock): StorageCache[F] =
    new StorageCache(new ConcurrentHashMap[UUID, StorageProjectCache[F]]())
}
