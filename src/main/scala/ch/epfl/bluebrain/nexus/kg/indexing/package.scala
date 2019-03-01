package ch.epfl.bluebrain.nexus.kg

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.async.ProjectCache
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._

package object indexing {
  type Identified[I, A] = (Id[I], A)
  implicit class ListResourcesSyntax[I, A](private val events: List[Identified[I, A]]) extends AnyVal {

    /**
      * Remove events with duplicated ''id''. In case of duplication found, the last element is kept and the previous removed.
      *
      * @return a new list without duplicated ids
      */
    def removeDupIds: List[A] =
      events.groupBy { case (id, _) => id }.values.flatMap(_.lastOption.map { case (_, elem) => elem }).toList
  }

  /**
    * Attempts to fetch the project from the cache and retries until it is found.
    *
    * @param projectRef the project unique reference
    * @tparam F the effect type
    * @return the project wrapped on the effect type
    */
  def fetchProject[F[_]](projectRef: ProjectRef)(implicit projectCache: ProjectCache[F],
                                                 retry: Retry[F, Throwable]): F[Project] =
    projectCache
      .get(projectRef)
      .mapRetry({ case Some(p) => p }, KgError.NotFound(Some(projectRef.show)): Throwable)
}
