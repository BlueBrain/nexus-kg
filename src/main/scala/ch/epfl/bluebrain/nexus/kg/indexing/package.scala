package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.kg.resources.Event

package object indexing {
  implicit class EventsSyntax(private val events: List[Event]) extends AnyVal {

    /**
      * Remove events with duplicated ''id''. In case of duplication found, the last event is kept and the previous removed.
      *
      * @return a new list of [[Event]] without duplicated ids. In case
      */
    def removeDupIds: List[Event] = events.groupBy(_.id).values.flatMap(_.lastOption).toList
  }
}
