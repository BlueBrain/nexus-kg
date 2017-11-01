package ch.epfl.bluebrain.nexus.kg.core.schemas

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for possible states of a schema.
  */
sealed trait SchemaState extends Product with Serializable

object SchemaState {

  /**
    * Initial state for all schemas.
    */
  final case object Initial extends SchemaState

  /**
    * State used for all schemas that have been created and later possibly updated, deprecated or published.
    *
    * @param id         the unique identifier of the schema
    * @param rev        the selected revision number
    * @param meta       the metadata associated to this schema
    * @param value      the json representation of the schema
    * @param published  the publish status
    * @param deprecated the deprecation status
    */
  final case class Current(id: SchemaId, rev: Long, meta: Meta, value: Json, published: Boolean, deprecated: Boolean)
      extends SchemaState

}
