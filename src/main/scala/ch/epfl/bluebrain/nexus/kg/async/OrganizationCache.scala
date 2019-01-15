package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Organization
import ch.epfl.bluebrain.nexus.kg.async.Cache._
import ch.epfl.bluebrain.nexus.kg.resources.OrganizationRef
import ch.epfl.bluebrain.nexus.service.indexer.cache.{KeyValueStore, KeyValueStoreConfig}

/**
  * The organization cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class OrganizationCache[F[_]] private (store: KeyValueStore[F, UUID, Organization])(implicit F: Monad[F])
    extends Cache[F, Organization](store) {

  private implicit val ordering: Ordering[Organization] = Ordering.by(_.label)

  /**
    * Attempts to fetch the organization resource with the provided ''label''
    *
    * @param label the organization label
    */
  def getBy(label: String): F[Option[Organization]] = store.findValue(_.label == label)

  /**
    * Attempts to fetch the organization with the provided ''ref''
    *
    * @param ref the organization unique reference
    */
  def get(ref: OrganizationRef): F[Option[Organization]] = super.get(ref.id)

  /**
    * Fetches all the organizations
    */
  def list(): F[List[Organization]] = store.values.map(_.toList.sorted)

  /**
    * Creates or replaces the organization with key provided uuid and value.
    *
    * @param value the organization value
    */
  def replace(value: Organization): F[Unit] = super.replace(value.uuid, value)

  /**
    * Deprecates the organization with the provided ref
    *
    * @param ref the organization unique reference
    * @param rev the organization new revision
    */
  def deprecate(ref: OrganizationRef, rev: Long): F[Unit] =
    store.computeIfPresent(ref.id, c => c.copy(rev = rev, deprecated = true)) *> F.unit

}
object OrganizationCache {

  /**
    * Creates a new organization index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): OrganizationCache[F] =
    new OrganizationCache(storeWrappedError[F, Organization]("organizations", _.rev))
}
