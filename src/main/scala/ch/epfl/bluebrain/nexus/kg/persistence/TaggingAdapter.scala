package ch.epfl.bluebrain.nexus.kg.persistence

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Id, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri

/**
  * A tagging event adapter that adds tags to discriminate between event hierarchies.
  */
class TaggingAdapter extends WriteEventAdapter {

  override def manifest(event: Any): String = ""

  def tagsFrom(id: Id[ProjectRef], types: Set[Iri.AbsoluteIri]): Set[String] =
    types.map(t => s"type=${t.show}") + s"project=${id.parent.id}"

  override def toJournal(event: Any): Any = event match {
    case Created(id, _, _, types, _, _, _) => Tagged(event, tagsFrom(id, types))
    case ev: Event                         => Tagged(ev, tagsFrom(ev.id, types = Set.empty))
    case _                                 => event
  }
}
