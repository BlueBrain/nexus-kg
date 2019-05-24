package ch.epfl.bluebrain.nexus.kg.cache

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig, OnKeyValueStoreChange}
import ch.epfl.bluebrain.nexus.kg.cache.Cache._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import shapeless.{TypeCase, Typeable}

class ViewCache[F[_]: Timer] private (projectToCache: ConcurrentHashMap[UUID, ViewProjectCache[F]])(
    implicit as: ActorSystem,
    F: Async[F],
    config: KeyValueStoreConfig) {

  /**
    * Fetches views for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[Set[View]] =
    getOrCreate(ref).get

  /**
    * Fetches the default Elastic Search view for the provided project.
    *
    * @param ref the project unique reference
    */
  def getDefaultElasticSearch(ref: ProjectRef): F[Option[ElasticSearchView]] =
    getBy[ElasticSearchView](ref, nxv.defaultElasticSearchIndex.value)

  /**
    * Fetches the default Sparql view for the provided project.
    *
    * @param ref the project unique reference
    */
  def getDefaultSparql(ref: ProjectRef): F[Option[SparqlView]] =
    getBy[SparqlView](ref, nxv.defaultSparqlIndex.value)

  /**
    * Fetches views filtered by type for the provided project.
    *
    * @param ref the project unique reference
    */
  def getBy[T <: View: Typeable](ref: ProjectRef): F[Set[T]] =
    getOrCreate(ref).getBy[T]

  /**
    * Fetches view of a specific type from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the view unique id in the provided project
    * @tparam T the type of view to be returned
    */
  def getBy[T <: View: Typeable](ref: ProjectRef, id: AbsoluteIri): F[Option[T]] =
    getOrCreate(ref).getBy[T](id)

  /**
    * Adds/updates or deprecates a view on the provided project.
    *
    * @param view the view value
    */
  def put(view: View): F[Unit] =
    getOrCreate(view.ref).put(view)

  /**
    * Adds a subscription to the cache
    *
    * @param ref   the project unique reference
    * @param value the method that gets triggered when a change to key value store occurs
    */
  def subscribe(ref: ProjectRef, value: OnKeyValueStoreChange[AbsoluteIri, View]): F[KeyValueStore.Subscription] =
    getOrCreate(ref).subscribe(value)

  private def getOrCreate(ref: ProjectRef): ViewProjectCache[F] =
    projectToCache.getSafe(ref.id).getOrElse(projectToCache.putAndReturn(ref.id, ViewProjectCache[F](ref)))
}

/**
  * The project view cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
private class ViewProjectCache[F[_]] private (store: KeyValueStore[F, AbsoluteIri, View])(implicit F: Monad[F])
    extends Cache[F, AbsoluteIri, View](store) {

  def get: F[Set[View]] = store.values

  def getBy[T <: View: Typeable]: F[Set[T]] = {
    val tpe = TypeCase[T]
    get.map(_.collect { case tpe(v) => v })
  }

  def getBy[T <: View: Typeable](id: AbsoluteIri): F[Option[T]] = {
    val tpe = TypeCase[T]
    get(id).map(_.collectFirst { case tpe(v) => v })
  }

  def put(view: View): F[Unit] =
    if (view.deprecated) store.remove(view.id) else store.put(view.id, view)

}

private object ViewProjectCache {

  def apply[F[_]: Timer](
      project: ProjectRef)(implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): ViewProjectCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    new ViewProjectCache(KeyValueStore.distributed(s"view-${project.id}", (_, view) => view.rev, mapError))(F)
  }
}

object ViewCache {

  def apply[F[_]: Async: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig): ViewCache[F] =
    new ViewCache(new ConcurrentHashMap[UUID, ViewProjectCache[F]]())
}
