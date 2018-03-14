package ch.epfl.bluebrain.nexus.kg.core.schemas

import io.circe.Json

/**
  * Data type representing the state of a schema.
  *
  * @param id         a unique identifier for the schema
  * @param rev        the selected revision for the schema
  * @param value      the current value of the schema
  * @param deprecated the deprecation status of the schema
  * @param published  the publish status of the schema
  */
final case class Schema(id: SchemaId, rev: Long, value: Json, deprecated: Boolean, published: Boolean)
