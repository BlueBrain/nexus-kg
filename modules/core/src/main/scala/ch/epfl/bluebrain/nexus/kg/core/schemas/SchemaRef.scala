package ch.epfl.bluebrain.nexus.kg.core.schemas

import ch.epfl.bluebrain.nexus.kg.core.Ref

/**
  * Reference data type to a specific schema revision.
  *
  * @param id  the unique identifier for the schema
  * @param rev a revision identifier for the schema
  */
final case class SchemaRef(id: SchemaId, rev: Long)

object SchemaRef {
  final implicit def schemaRef(ref: SchemaRef): Ref[SchemaId] = Ref(ref.id, ref.rev)
}