package ch.epfl.bluebrain.nexus.kg.core.schemas.shapes

import io.circe.Json

/**
  * Data type representing the state of a shape.
  *
  * @param id         a unique identifier for the shape
  * @param rev        the selected revision for the schema (and the shape)
  * @param value      the current value of the shape
  * @param deprecated the deprecation status of the schema (and the shape)
  * @param published  the publish status of the schema (and the shape)
  */
final case class Shape(id: ShapeId, rev: Long, value: Json, deprecated: Boolean, published: Boolean)
