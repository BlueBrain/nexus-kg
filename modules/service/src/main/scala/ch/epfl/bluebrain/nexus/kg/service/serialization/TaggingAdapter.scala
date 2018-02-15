package ch.epfl.bluebrain.nexus.kg.service.serialization

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import ch.epfl.bluebrain.nexus.kg.service.contexts.ContextEvent
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectEvent
import ch.epfl.bluebrain.nexus.kg.service.schemas.SchemaEvent

/**
  * A tagging event adapter that adds tags to discriminate between event hierarchies.
  */
class TaggingAdapter extends WriteEventAdapter {

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case ev: ProjectEvent => Tagged(ev, Set("project"))
    case ev: SchemaEvent  => Tagged(ev, Set("schema"))
    case ev: ContextEvent => Tagged(ev, Set("context"))
    case _                => event
  }
}
