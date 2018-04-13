package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.{JsonLDPayload, JsonPayload, TurtlePayload}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment
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
                          distribution: Set[Attachment],
                          deprecated: Boolean)

object Resource {

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
        val json = id.asJson deepMerge value.asJson deepMerge Json.obj("rev" -> Json.fromLong(rev),
                                                                       "deprecated" -> Json.fromBoolean(deprecated))
        if (distribution.isEmpty) json
        else json deepMerge Json.obj("distribution" -> distribution.asJson)
    }
}
