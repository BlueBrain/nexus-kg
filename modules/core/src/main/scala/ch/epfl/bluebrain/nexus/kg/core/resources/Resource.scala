package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.{JsonLDPayload, JsonPayload, TurtlePayload}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.BinaryAttributes
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}

/**
  * The information exposed by the bundle API related to a resource.
  * It is a subset of the information available on [[ch.epfl.bluebrain.nexus.kg.core.resources.State.Current]]
  *
  * @param id           the identifier of the resource
  * @param rev          the selected revision number
  * @param value        the payload of the resource
  * @param distribution the attachment's metadata of the resource
  * @param deprecated   the deprecation status
  */
final case class Resource(id: RepresentationId,
                          rev: Long,
                          value: Payload,
                          distribution: Set[BinaryAttributes],
                          deprecated: Boolean)

object Resource {

  private implicit val attachmentEncoder: Encoder[BinaryAttributes] =
    Encoder.forProduct4("originalFileName", "mediaType", "contentSize", "digest")(at =>
      (at.filename, at.mediaType, at.contentSize, at.digest))

  private implicit val payloadEnc: Encoder[Payload] =
    Encoder.instance {
      case JsonPayload(v)   => v
      case JsonLDPayload(v) => v
      case TurtlePayload(str) =>
        Json.obj("body" -> Json.fromString(str), "format" -> Json.fromString("turtle"))
    }

  implicit def resourceEncoder(implicit config: AdminConfig): Encoder[Resource] =
    Encoder.instance {
      case Resource(id, rev, value, distribution, deprecated) =>
        val distJson = if (distribution.isEmpty) Json.obj() else Json.obj("distribution" -> distribution.asJson)
        val meta     = Json.obj("rev" -> Json.fromLong(rev), "deprecated" -> Json.fromBoolean(deprecated))
        id.asJson deepMerge value.asJson deepMerge meta deepMerge distJson
    }
}
