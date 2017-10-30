package ch.epfl.bluebrain.nexus.kg.service.persistence

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent

/**
  * A tagging event adapter that adds tags to discriminate between event hierarchies.
  */
class TaggingAdapter extends WriteEventAdapter {

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case ev: OrgEvent      => Tagged(ev, Set("organization"))
    case ev: DomainEvent   => Tagged(ev, Set("domain"))
    case ev: SchemaEvent   => Tagged(ev, Set("schema"))
    case ev: ContextEvent  => Tagged(ev, Set("context"))
    case ev: InstanceEvent => Tagged(ev, Set("instance"))
    case _                 => event
  }
}
