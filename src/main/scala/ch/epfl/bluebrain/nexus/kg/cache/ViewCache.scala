package ch.epfl.bluebrain.nexus.kg.cache

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig, OnKeyValueStoreChange}
import ch.epfl.bluebrain.nexus.kg.cache.Cache._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import shapeless.{TypeCase, Typeable}

class ViewCache[F[_]: Effect: Timer] private (projectToCache: ConcurrentHashMap[UUID, ViewProjectCache[F]])(
    implicit as: ActorSystem,
    config: KeyValueStoreConfig
) {

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
    * Fetches a projection from a view of a specific type from the provided project and with the provided id
    *
    * @param ref          the project unique reference
    * @param viewId       the composite view unique id in the provided project
    * @param projectionId the id of the projection
    * @tparam T the type of view to be returned
    */
  def getProjectionBy[T <: View: Typeable](
      ref: ProjectRef,
      viewId: AbsoluteIri,
      projectionId: AbsoluteIri
  ): F[Option[T]] =
    getViewAndProjectionBy[T](ref, viewId, projectionId).map(_.map { case (_, value) => value })

  /**
    * Fetches a compositeView and a projection within it
    *
    * @param ref          the project unique reference
    * @param viewId       the composite view unique id in the provided project
    * @param projectionId the id of the projection
    * @tparam T the type of view to be returned
    */
  def getViewAndProjectionBy[T <: View: Typeable](
      ref: ProjectRef,
      viewId: AbsoluteIri,
      projectionId: AbsoluteIri
  ): F[Option[(CompositeView, T)]] = {
    val tpe = TypeCase[T]
    getBy[CompositeView](ref, viewId).map { viewOpt =>
      viewOpt.flatMap { view =>
        val projections = view.projections.map(_.view) + view.defaultSparqlView
        projections.collectFirst {
          case tpe(v) if v.id == projectionId => view -> v
        }
      }
    }
  }

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
private class ViewProjectCache[F[_]: Monad] private (store: KeyValueStore[F, AbsoluteIri, View])
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

  def apply[F[_]: Effect: Timer](
      project: ProjectRef
  )(implicit as: ActorSystem, config: KeyValueStoreConfig): ViewProjectCache[F] =
    new ViewProjectCache(KeyValueStore.distributed(s"view-${project.id}", (_, view) => view.rev))
}

object ViewCache {

  def apply[F[_]: Effect: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig): ViewCache[F] =
    new ViewCache(new ConcurrentHashMap[UUID, ViewProjectCache[F]]())
}
