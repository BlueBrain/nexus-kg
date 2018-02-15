package ch.epfl.bluebrain.nexus.kg.service.contexts

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent._
import ch.epfl.bluebrain.nexus.kg.service.types.Revisioned
import io.circe.Json
import shapeless.Typeable

/**
  * Enumeration type for all events that are emitted for contexts.
  */
sealed trait ContextEvent extends Product with Serializable with Revisioned {

  /**
    * @return the unique identifier of the context
    */
  def id: ContextId

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object ContextEvent {

  private val jsonType = implicitly[Typeable[Json]]

  /**
    * Evidence that a context has been created.
    *
    * @param id    the unique identifier of the context
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the json representation of the context
    */
  final case class ContextCreated(id: ContextId, rev: Long, meta: Meta, value: Json) extends ContextEvent

  /**
    * Evidence that a context has been updated.
    *
    * @param id    the unique identifier of the context
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new json representation of the context
    */
  final case class ContextUpdated(id: ContextId, rev: Long, meta: Meta, value: Json) extends ContextEvent

  /**
    * Evidence that a context has been deprecated.
    *
    * @param id   the unique identifier of the context
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class ContextDeprecated(id: ContextId, rev: Long, meta: Meta) extends ContextEvent

  implicit def toResourceEvent(contextEvent: ContextEvent): ResourceEvent[ContextId] = contextEvent match {
    case ContextCreated(id, rev, meta, value) => ResourceCreated(id, rev, meta, value)
    case ContextUpdated(id, rev, meta, value) => ResourceUpdated(id, rev, meta, value)
    case ContextDeprecated(id, rev, meta)     => ResourceDeprecated(id, rev, meta)
  }

  implicit def fromResourceEvent(res: ResourceEvent[ContextId]): Option[ContextEvent] = res match {
    case ResourceCreated(id, rev, meta, value) => jsonType.cast(value).map(ContextCreated(id, rev, meta, _))
    case ResourceUpdated(id, rev, meta, value) => jsonType.cast(value).map(ContextUpdated(id, rev, meta, _))
    case ResourceDeprecated(id, rev, meta)     => Some(ContextDeprecated(id, rev, meta))

  }

}