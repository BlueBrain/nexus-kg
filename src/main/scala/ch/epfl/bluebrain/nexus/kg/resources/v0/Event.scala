package ch.epfl.bluebrain.nexus.kg.resources.v0
import io.circe.generic.extras.Configuration
import io.circe.{Decoder, Json}
import io.circe.generic.extras.semiauto._

/**
  * Enumeration for all v0 event types that are supported by
  * the [[ch.epfl.bluebrain.nexus.kg.indexing.MigrationIndexer]]
  */
sealed trait Event {
  def id: String
  def rev: Long
  def meta: Meta
}

object Event {
  final case class InstanceCreated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class InstanceUpdated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class InstanceDeprecated(id: String, rev: Long, meta: Meta) extends Event

  final case class InstanceAttachmentCreated(id: String, rev: Long, meta: Meta, value: Attachment.Meta) extends Event

  final case class InstanceAttachmentRemoved(id: String, rev: Long, meta: Meta) extends Event

  final case class SchemaCreated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class SchemaUpdated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class SchemaPublished(id: String, rev: Long, meta: Meta) extends Event

  final case class SchemaDeprecated(id: String, rev: Long, meta: Meta) extends Event

  final case class ContextCreated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class ContextUpdated(id: String, rev: Long, meta: Meta, value: Json) extends Event

  final case class ContextPublished(id: String, rev: Long, meta: Meta) extends Event

  final case class ContextDeprecated(id: String, rev: Long, meta: Meta) extends Event

  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  implicit def eventDecoder: Decoder[Event] = deriveDecoder[Event]
}
