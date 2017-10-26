package ch.epfl.bluebrain.nexus.kg.core.contexts

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
}

object ContextEvent {

  /**
    * Evidence that a Context has been created.
    *
    * @param id    the unique identifier of the Context
    * @param rev   the revision number that this event generates
    * @param value the json representation of the Context
    */
  final case class ContextCreated(id: ContextId, rev: Long, value: Json) extends ContextEvent

  /**
    * Evidence that a Context has been updated.
    *
    * @param id    the unique identifier of the Context
    * @param rev   the revision number that this event generates
    * @param value the new json representation of the Context
    */
  final case class ContextUpdated(id: ContextId, rev: Long, value: Json) extends ContextEvent

  /**
    * Evidence that a Context has been published.
    *
    * @param id  the unique identifier of the Context
    * @param rev the revision number that this event generates
    */
  final case class ContextPublished(id: ContextId, rev: Long) extends ContextEvent

  /**
    * Evidence that a Context has been deprecated.
    *
    * @param id  the unique identifier of the Context
    * @param rev the revision number that this event generates
    */
  final case class ContextDeprecated(id: ContextId, rev: Long) extends ContextEvent


}