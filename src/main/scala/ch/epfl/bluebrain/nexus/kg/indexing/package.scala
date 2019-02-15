package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.kg.resources.Id

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
}
