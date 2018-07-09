package ch.epfl.bluebrain.nexus.kg.core.persistence

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets.UTF_8

import akka.serialization.SerializerWithStringManifest
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")
  private final val EventName                = "event"
  private val enc                            = deriveEncoder[Event]
  private val dec                            = deriveDecoder[Event]

  private implicit val encodeReprId: Encoder[RepresentationId] =
    Encoder.forProduct3("project", "id", "schema")(r => (r.projectRef, r.resourceId, r.schemaId))

  private implicit val decodeReprId: Decoder[RepresentationId] =
    Decoder.forProduct3("project", "id", "schema")(RepresentationId.apply)

  private implicit val eventEncoder: Encoder[Event] =
    Encoder.instance { ev =>
      enc(ev).removeKeys("id") deepMerge ev.id.asJson
    }

  private implicit val eventDecoder: Decoder[Event] =
    Decoder.instance { hc =>
      val json = hc.value
      for {
        key <- json.as[RepresentationId]
        combined = json deepMerge Json.obj("id" -> key.asJson)
        event <- dec(combined.hcursor)
      } yield event
    }

  class EventSerializer extends SerializerWithStringManifest {
    override final def manifest(o: AnyRef): String =
      o match {
        case _: Event => EventName
        case _        => throw new IllegalArgumentException(s"Unable to generate manifest for '$o'")
      }

    override final def toBinary(o: AnyRef): Array[Byte] =
      o match {
        case ev: Event => ev.asJson.noSpaces.getBytes(UTF_8)
        case _         => throw new IllegalArgumentException(s"Unable to encode to binary; unknown type '$o'")

      }

    override final def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      manifest match {
        case `EventName` =>
          decode[Event](new String(bytes, UTF_8)) match {
            case Left(err)    => throw err
            case Right(value) => value
          }
        case _ => throw new NotSerializableException(manifest)
      }
    //"nexus-json".getBytes.map(_.toInt).sum
    val identifier = 1050

  }
}
