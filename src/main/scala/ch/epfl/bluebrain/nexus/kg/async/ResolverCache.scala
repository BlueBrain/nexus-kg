package ch.epfl.bluebrain.nexus.kg.async

import java.time.Clock
import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig}
import ch.epfl.bluebrain.nexus.kg.async.Cache._
import ch.epfl.bluebrain.nexus.kg.async.ResolverCache.RevisionedResolvers
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * The resolver cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class ResolverCache[F[_]] private (store: KeyValueStore[F, UUID, RevisionedResolvers])(implicit F: Monad[F],
                                                                                       clock: Clock)
    extends Cache[F, UUID, RevisionedResolvers](store) {

  private implicit val ordering: Ordering[Resolver] = Ordering.by(_.priority)

  private implicit def toRevisioned(resolvers: List[Resolver]): RevisionedResolvers =
    RevisionedValue(clock.instant().toEpochMilli, resolvers)

  /**
    * Fetches resolvers for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[List[Resolver]] = get(ref.id).map(_.map(_.value.sorted).getOrElse(List.empty))

  /**
    * Fetches resolver from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the resolver unique id in the provided project
    */
  def get(ref: ProjectRef, id: AbsoluteIri): F[Option[Resolver]] =
    get(ref.id).map(_.collectFirstSome { r =>
      r.value.find(_.id == id)
    })

  /**
    * Adds/updates or deprecates a resolver on the provided project.
    *
    * @param value the resolver value
    */
  def put(value: Resolver): F[Unit] =
    if (value.deprecated) remove(value)
    else add(value)

  private def add(resolver: Resolver): F[Unit] =
    store.get(resolver.ref.id) flatMap {
      case Some(RevisionedValue(_, list)) => store.put(resolver.ref.id, resolver :: list.filterNot(_.id == resolver.id))
      case None                           => store.put(resolver.ref.id, List(resolver))
    }

  private def remove(resolver: Resolver): F[Unit] =
    store.computeIfPresent(resolver.ref.id, _.value.filterNot(_.id == resolver.id)) *> F.unit

}

object ResolverCache {

  type RevisionedResolvers = RevisionedValue[List[Resolver]]

  /**
    * Creates a new view index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem,
                         config: KeyValueStoreConfig,
                         F: Async[F],
                         clock: Clock): ResolverCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    val function: (Long, RevisionedResolvers) => Long = { case (_, res) => res.rev }
    new ResolverCache(KeyValueStore.distributed("resolvers", function, mapError))(F, clock)
  }

}
