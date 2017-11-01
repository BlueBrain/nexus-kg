package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for all events that are emitted for schemas.
  */
sealed trait ContextEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the context
    */
  def id: ContextId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object ContextEvent {

  /**
    * Evidence that a Context has been created.
    *
    * @param id    the unique identifier of the Context
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the json representation of the Context
    */
  final case class ContextCreated(id: ContextId, rev: Long, meta: Meta, value: Json) extends ContextEvent

  /**
    * Evidence that a Context has been updated.
    *
    * @param id    the unique identifier of the Context
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new json representation of the Context
    */
  final case class ContextUpdated(id: ContextId, rev: Long, meta: Meta, value: Json) extends ContextEvent

  /**
    * Evidence that a Context has been published.
    *
    * @param id   the unique identifier of the Context
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class ContextPublished(id: ContextId, rev: Long, meta: Meta) extends ContextEvent

  /**
    * Evidence that a Context has been deprecated.
    *
    * @param id   the unique identifier of the Context
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class ContextDeprecated(id: ContextId, rev: Long, meta: Meta) extends ContextEvent

}
