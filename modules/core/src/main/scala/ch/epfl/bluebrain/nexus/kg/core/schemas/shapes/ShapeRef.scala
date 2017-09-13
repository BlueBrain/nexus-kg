package ch.epfl.bluebrain.nexus.kg.core.schemas.shapes

import ch.epfl.bluebrain.nexus.kg.core.Ref

/**
  * Reference data type to a specific shape revision.
  *
  * @param id  the unique identifier for the shape
  * @param rev a revision identifier for the schema (and the shape)
  */
final case class ShapeRef(id: ShapeId, rev: Long)

object ShapeRef {
  final implicit def shapeRef(ref: ShapeRef): Ref[ShapeId] = Ref(ref.id, ref.rev)
}
