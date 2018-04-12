package ch.epfl.bluebrain.nexus.kg.core.persistence

import java.io.NotSerializableException

import akka.serialization.SerializerWithStringManifest
import ch.epfl.bluebrain.nexus.kg.core.resources.{Event, Key}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.java8.time._
import io.circe.parser.decode
import io.circe.syntax._
import java.nio.charset.StandardCharsets.UTF_8

import io.circe.generic.extras.semiauto.deriveEncoder
import io.circe.generic.extras.semiauto.deriveDecoder
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import io.circe.{Decoder, Encoder, Json}

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")
  private final val EventName                = "event"

  private implicit val eventEncoder: Encoder[Event] = {
    val enc = deriveEncoder[Event]
    Encoder.instance { ev =>
      enc(ev).removeKeys("id") deepMerge ev.id.asJson
    }
  }

  private implicit val eventDecoder: Decoder[Event] = {
    val dec = deriveDecoder[Event]
    Decoder.decodeJson.emap { json =>
      val hc = json.hcursor
      val result = for {
        id      <- hc.get[String]("@id")
        project <- hc.get[String]("project")
        schema  <- hc.get[String]("schema")
        combined = json deepMerge Json.obj("id" -> Key(project, id, schema).asJson)
        event <- dec(combined.hcursor)
      } yield (event)
      result.left.map(_.message)
    }
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

    val identifier = 1234567

  }
}
