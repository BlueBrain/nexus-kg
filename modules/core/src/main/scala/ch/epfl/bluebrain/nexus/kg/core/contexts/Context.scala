package ch.epfl.bluebrain.nexus.kg.core.contexts

import io.circe.Json

/**
  * Data type representing the state of a context.
  *
  * @param id         a unique identifier for the context
  * @param rev        the selected revision for the context
  * @param value      the current value of the context
  * @param deprecated the deprecation status of the context
  * @param published  the publish status of the context
  */
final case class Context(id: ContextId, rev: Long, value: Json, deprecated: Boolean, published: Boolean)