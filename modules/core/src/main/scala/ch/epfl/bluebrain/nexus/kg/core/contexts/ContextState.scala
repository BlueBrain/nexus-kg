package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for possible states of a context.
  */
sealed trait ContextState extends Product with Serializable

object ContextState {

  /**
    * Initial state for all contexts.
    */
  final case object Initial extends ContextState

  /**
    * State used for all contexts that have been created and later possibly updated, deprecated or published.
    *
    * @param id         the unique identifier of the context
    * @param rev        the selected revision number
    * @param meta       the metadata associated to this context
    * @param value      the json representation of the context
    * @param published  the publish status
    * @param deprecated the deprecation status
    */
  final case class Current(id: ContextId, rev: Long, meta: Meta, value: Json, published: Boolean, deprecated: Boolean)
      extends ContextState

}
