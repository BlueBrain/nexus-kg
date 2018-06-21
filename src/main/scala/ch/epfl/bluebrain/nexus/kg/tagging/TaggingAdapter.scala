package ch.epfl.bluebrain.nexus.kg.tagging

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import ch.epfl.bluebrain.nexus.kg.resources.Event

/**
  * A tagging event adapter that adds tags to discriminate between event hierarchies.
  */
class TaggingAdapter extends WriteEventAdapter {

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case ev: Event => Tagged(ev, Set(ev.id.parent.id))
    case _         => event
  }
}
