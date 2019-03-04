package ch.epfl.bluebrain.nexus.kg.async

import java.time.{Clock, Instant}
import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig}
import ch.epfl.bluebrain.nexus.kg.async.Cache.mapError
import ch.epfl.bluebrain.nexus.kg.async.StorageCache._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * The storage cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class StorageCache[F[_]] private (store: KeyValueStore[F, UUID, RevisionedStorages])(implicit F: Monad[F], clock: Clock)
    extends Cache[F, UUID, RevisionedStorages](store) {

  private implicit val ordering: Ordering[RevisionedStorage] = Ordering.by((s: RevisionedStorage) => s.rev).reverse

  private implicit def toRevisioned(storages: List[RevisionedStorage])(implicit instant: Instant): RevisionedStorages =
    RevisionedValue(instant.toEpochMilli, storages)

  private def revisioned(storage: Storage)(implicit instant: Instant): RevisionedStorage =
    RevisionedValue(instant.toEpochMilli, storage)

  /**
    * Fetches the storages for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[List[Storage]] =
    get(ref.id).map(_.map(_.value.sorted.map(_.value)).getOrElse(List.empty))

  /**
    * Fetches the default storage from the provided project
    *
    * @param ref the project unique reference
    */
  def getDefault(ref: ProjectRef): F[Option[Storage]] =
    get(ref).map(_.collectFirst { case storage if storage.default => storage })

  /**
    * Fetches the storage from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the storage unique id in the provided project
    */
  def get(ref: ProjectRef, id: AbsoluteIri): F[Option[Storage]] =
    get(ref.id).map(_.collectFirstSome {
      _.value.collectFirst { case RevisionedValue(_, storage) if storage.id == id => storage }
    })

  /**
    * Adds/updates or deprecates a storage on the provided project.
    *
    * @param storage the storage value
    */
  def put(storage: Storage)(implicit instant: Instant = clock.instant()): F[Unit] =
    if (storage.deprecated) remove(storage)
    else add(storage)

  private def add(storage: Storage)(implicit instant: Instant): F[Unit] =
    store.get(storage.ref.id) flatMap {
      case Some(RevisionedValue(_, list)) =>
        store.put(storage.ref.id, revisioned(storage) :: list.filterNot(_.value.id == storage.id))
      case None => store.put(storage.ref.id, List(revisioned(storage)))
    }

  private def remove(storage: Storage)(implicit instant: Instant): F[Unit] =
    store.computeIfPresent(storage.ref.id, _.value.filterNot(_.value.id == storage.id)) *> F.unit

}

object StorageCache {

  type RevisionedStorages = RevisionedValue[List[RevisionedStorage]]
  type RevisionedStorage  = RevisionedValue[Storage]

  /**
    * Creates a new storage index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem,
                         config: KeyValueStoreConfig,
                         F: Async[F],
                         clock: Clock): StorageCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    val function: (Long, RevisionedStorages) => Long = { case (_, res) => res.rev }
    new StorageCache(KeyValueStore.distributed("storage", function, mapError))(F, clock)
  }

}
