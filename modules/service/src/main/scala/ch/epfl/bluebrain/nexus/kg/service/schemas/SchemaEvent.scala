package ch.epfl.bluebrain.nexus.kg.service.schemas

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent._
import ch.epfl.bluebrain.nexus.kg.service.types.Revisioned
import io.circe.Json
import shapeless.Typeable

/**
  * Enumeration type for all events that are emitted for schemas.
  */
sealed trait SchemaEvent extends Product with Serializable with Revisioned {

  /**
    * @return the unique identifier of the schema
    */
  def id: SchemaId

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object SchemaEvent {

  private val jsonType = implicitly[Typeable[Json]]

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
    * Evidence that a schema has been deprecated.
    *
    * @param id   the unique identifier of the schema
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class SchemaDeprecated(id: SchemaId, rev: Long, meta: Meta) extends SchemaEvent

  implicit def toResourceEvent(schemaEvent: SchemaEvent): ResourceEvent[SchemaId] = schemaEvent match {
    case SchemaCreated(id, rev, meta, value) => ResourceCreated(id, rev, meta, value)
    case SchemaUpdated(id, rev, meta, value) => ResourceUpdated(id, rev, meta, value)
    case SchemaDeprecated(id, rev, meta)     => ResourceDeprecated(id, rev, meta)
  }

  implicit def fromResourceEvent(res: ResourceEvent[SchemaId]): Option[SchemaEvent] = res match {
    case ResourceCreated(id, rev, meta, value) => jsonType.cast(value).map(SchemaCreated(id, rev, meta, _))
    case ResourceUpdated(id, rev, meta, value) => jsonType.cast(value).map(SchemaUpdated(id, rev, meta, _))
    case ResourceDeprecated(id, rev, meta)     => Some(SchemaDeprecated(id, rev, meta))

  }

}
