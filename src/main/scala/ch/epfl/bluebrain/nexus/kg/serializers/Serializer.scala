package ch.epfl.bluebrain.nexus.kg.serializers

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets.UTF_8

import akka.serialization.SerializerWithStringManifest
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, RelativeIri}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.java8.time._
import io.circe.parser.decode
import io.circe.refined._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  private implicit val config: Configuration              = Configuration.default.withDiscriminator("type")
  private final val EventName                             = "event"
  private implicit val identityEncoder: Encoder[Identity] = deriveEncoder[Identity]
  private implicit val identityDecoder: Decoder[Identity] = deriveDecoder[Identity]

  private implicit def refEncoder: Encoder[Ref] = Encoder { ref =>
    Json.fromString(ref.iri.show)
  }
  private implicit def refDecoder: Decoder[Ref] = iriDecoder.map(Ref(_))

  private implicit def iriEncoder: Encoder[AbsoluteIri] = Encoder { iri =>
    Json.fromString(iri.show)
  }
  private implicit def iriDecoder: Decoder[AbsoluteIri] = Decoder.decodeString.emapTry { iri =>
    Iri.absolute(iri).left.map(err => new IllegalArgumentException(err)).toTry
  }

  private implicit def relativeIriEncoder: Encoder[RelativeIri] = Encoder { iri =>
    Json.fromString(iri.show)
  }
  private implicit def relativeIriDecoder: Decoder[RelativeIri] = Decoder.decodeString.emapTry { iri =>
    RelativeIri.apply(iri).left.map(err => new IllegalArgumentException(err)).toTry
  }

  private implicit def binaryAttributesEncoder: Encoder[BinaryAttributes] = deriveEncoder[BinaryAttributes]

  private implicit def binaryAttributesDecoder: Decoder[BinaryAttributes] = deriveDecoder[BinaryAttributes]

  private implicit val encodeResId: Encoder[ResId] =
    Encoder.forProduct2("project", "id")(r => (r.parent.id, r.value.show))

  private implicit val decodeResId: Decoder[ResId] =
    Decoder.forProduct2("project", "id") { (proj: String, id: AbsoluteIri) =>
      Id(ProjectRef(proj), id)
    }

  private val enc = deriveEncoder[Event]
  private val dec = deriveDecoder[Event]

  private implicit val eventEncoder: Encoder[Event] =
    Encoder.instance { ev =>
      enc(ev).removeKeys("id") deepMerge ev.id.asJson
    }

  private implicit val eventDecoder: Decoder[Event] =
    Decoder.instance { hc =>
      val json = hc.value
      for {
        key <- json.as[ResId]
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
