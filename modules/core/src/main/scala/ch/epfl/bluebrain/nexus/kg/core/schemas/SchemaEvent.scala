package ch.epfl.bluebrain.nexus.kg.core.schemas

import io.circe.Json
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta

/**
  * Enumeration type for all events that are emitted for schemas.
  */
sealed trait SchemaEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the schema
    */
  def id: SchemaId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object SchemaEvent {

  /**
    * Evidence that a schema has been created.
    *
    * @param id    the unique identifier of the schema
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the json representation of the schema
    */
  final case class SchemaCreated(id: SchemaId, rev: Long, meta: Meta, value: Json) extends SchemaEvent

  /**
    * Evidence that a schema has been updated.
    *
    * @param id    the unique identifier of the schema
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new json representation of the schema
    */
  final case class SchemaUpdated(id: SchemaId, rev: Long, meta: Meta, value: Json) extends SchemaEvent

  /**
    * Evidence that a schema has been published.
    *
    * @param id   the unique identifier of the schema
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class SchemaPublished(id: SchemaId, rev: Long, meta: Meta) extends SchemaEvent

  /**
    * Evidence that a schema has been deprecated.
    *
    * @param id   the unique identifier of the schema
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class SchemaDeprecated(id: SchemaId, rev: Long, meta: Meta) extends SchemaEvent

}
