package ch.epfl.bluebrain.nexus.kg.async

import java.time.Clock
import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.cache.{KeyValueStore, KeyValueStoreConfig}
import ch.epfl.bluebrain.nexus.kg.async.Cache.mapError
import ch.epfl.bluebrain.nexus.kg.async.ViewCache._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import shapeless.{TypeCase, Typeable}

/**
  * The view cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class ViewCache[F[_]] private (store: KeyValueStore[F, UUID, RevisionedViews])(implicit F: Monad[F], clock: Clock)
    extends Cache[F, UUID, RevisionedViews](store) {

  private implicit def toRevisioned(views: Set[View]): RevisionedViews =
    RevisionedValue(clock.instant().toEpochMilli, views)

  /**
    * Fetches views for the provided project.
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[Set[View]] = super.get(ref.id).map(_.map(_.value).getOrElse(Set.empty))

  /**
    * Fetches views filtered by type for the provided project.
    *
    * @param ref the project unique reference
    */
  def getBy[T <: View: Typeable](ref: ProjectRef): F[Set[T]] = {
    val tpe = TypeCase[T]

    super.get(ref.id).map {
      case Some(RevisionedValue(_, views)) => views.collect { case tpe(v) => v }
      case _                               => Set.empty[T]
    }
  }

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
      case Some(RevisionedValue(_, views)) => store.put(view.ref.id, views.filterNot(_.id == view.id) + view)
      case None                            => store.put(view.ref.id, Set(view))
    }

  private def remove(view: View): F[Unit] =
    store.computeIfPresent(view.ref.id, _.value.filterNot(_.id == view.id)) *> F.unit

}

object ViewCache {

  type RevisionedViews = RevisionedValue[Set[View]]

  /**
    * Creates a new view index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem,
                         config: KeyValueStoreConfig,
                         F: Async[F],
                         clock: Clock): ViewCache[F] = {
    import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    val function: (Long, RevisionedViews) => Long = { case (_, res) => res.rev }
    new ViewCache(KeyValueStore.distributed("views", function, mapError))(F, clock)
  }
}
