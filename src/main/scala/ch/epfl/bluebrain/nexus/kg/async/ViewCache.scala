package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.async.Cache.storeWrappedError
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.indexer.cache.{KeyValueStore, KeyValueStoreConfig}
import shapeless.{TypeCase, Typeable}

/**
  * The view cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class ViewCache[F[_]] private (store: KeyValueStore[F, UUID, Set[View]])(implicit F: Monad[F])
    extends Cache[F, Set[View]](store) {

  /**
    * Fetches views for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[Set[View]] = super.get(ref.id).map(_.getOrElse(Set.empty))

  /**
    * Fetches view of a specific type from the provided project and with the provided id
    *
    * @param ref the project unique reference
    * @param id  the view unique id in the provided project
    * @tparam T the type of view to be returned
    */
  def getBy[T <: View: Typeable](ref: ProjectRef, id: AbsoluteIri): F[Option[T]] = {
    val tpe = TypeCase[T]
    get(ref).map(_.collectFirst { case tpe(v) if v.id == id => v })
  }

  /**
    * Adds/updates or deprecates a view on the provided project.
    *
    * @param value the view value
    */
  def put(value: View): F[Unit] =
    if (value.deprecated) remove(value)
    else add(value)

  private def add(view: View): F[Unit] =
    store.get(view.ref.id) flatMap {
      case Some(views) => store.put(view.ref.id, views.filterNot(_.id == view.id) + view)
      case None        => store.put(view.ref.id, Set(view))
    }

  private def remove(view: View): F[Unit] =
    store.computeIfPresent(view.ref.id, _.filterNot(_.id == view.id)) *> F.unit

}

object ViewCache {

  /**
    * Creates a new view index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): ViewCache[F] =
    new ViewCache[F](storeWrappedError[F, Set[View]]("views", _.foldLeft(0L)(_ + _.rev)))
}
