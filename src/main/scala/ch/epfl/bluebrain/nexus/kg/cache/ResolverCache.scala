package ch.epfl.bluebrain.nexus.kg.cache

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig}
import ch.epfl.bluebrain.nexus.kg.cache.Cache._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

class ResolverCache[F[_]: Timer] private (projectToCache: ConcurrentHashMap[UUID, ResolverProjectCache[F]])(
    implicit as: ActorSystem,
    F: Async[F],
    config: KeyValueStoreConfig
) {

  /**
    * Fetches resolvers for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[List[Resolver]] =
    getOrCreate(ref).get

  /**
    * Fetches resolver from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the view unique id in the provided project
    */
  def get(ref: ProjectRef, id: AbsoluteIri): F[Option[Resolver]] =
    getOrCreate(ref).get(id)

  /**
    * Adds/updates or deprecates a resolver on the provided project.
    *
    * @param resolver the storage value
    */
  def put(resolver: Resolver): F[Unit] =
    getOrCreate(resolver.ref).put(resolver)

  private def getOrCreate(ref: ProjectRef): ResolverProjectCache[F] =
    projectToCache.getSafe(ref.id).getOrElse(projectToCache.putAndReturn(ref.id, ResolverProjectCache[F](ref)))

}

/**
  * The resolver cache for a project backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
private class ResolverProjectCache[F[_]] private (store: KeyValueStore[F, AbsoluteIri, Resolver])(implicit F: Monad[F])
    extends Cache[F, AbsoluteIri, Resolver](store) {

  private implicit val ordering: Ordering[Resolver] = Ordering.by(_.priority)

  def get: F[List[Resolver]] = store.values.map(_.toList.sorted)

  def put(resolver: Resolver): F[Unit] =
    if (resolver.deprecated) store.remove(resolver.id)
    else store.put(resolver.id, resolver)

}

private object ResolverProjectCache {

  def apply[F[_]: Timer](
      project: ProjectRef
  )(implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): ResolverProjectCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    new ResolverProjectCache(
      KeyValueStore.distributed(s"resolver-${project.id}", (_, resolver) => resolver.rev, mapError)
    )(F)
  }

}

object ResolverCache {

  def apply[F[_]: Async: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig): ResolverCache[F] =
    new ResolverCache(new ConcurrentHashMap[UUID, ResolverProjectCache[F]]())
}
