package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for commands that apply to contexts.
  */
sealed trait ContextCommand extends Product with Serializable {

  /**
    * @return the unique identifier for the context for which this command will be evaluated
    */
  def id: ContextId

  /**
    * @return the metadata associated to this command
    */
  def meta: Meta
}

object ContextCommand {

  /**
    * Command that signals the intent to create a new context.
    *
    * @param id    the unique identifier for the context to be created
    * @param meta  the metadata associated to this command
    * @param value the json representation of the context
    */
  final case class CreateContext(id: ContextId, meta: Meta, value: Json) extends ContextCommand

  /**
    * Command that signals the intent to update a context value.
    *
    * @param id    the unique identifier for the context to be updated
    * @param rev   the last known revision of the context
    * @param meta  the metadata associated to this command
    * @param value the new json representation of the context
    */
  final case class UpdateContext(id: ContextId, rev: Long, meta: Meta, value: Json) extends ContextCommand

  /**
    * Command that signals the intent to publish a context.
    *
    * @param id   the unique identifier for the context to be published
    * @param rev  the last known revision of the context
    * @param meta the metadata associated to this command
    */
  final case class PublishContext(id: ContextId, rev: Long, meta: Meta) extends ContextCommand

  /**
    * Command that signals the intent to deprecate a context.
    *
    * @param id   the unique identifier for the context to be deprecated
    * @param rev  the last known revision of the context
    * @param meta the metadata associated to this command
    */
  final case class DeprecateContext(id: ContextId, rev: Long, meta: Meta) extends ContextCommand

}
