package ch.epfl.bluebrain.nexus.kg.serializers

import java.nio.file.{Path, Paths}

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.serialization.AkkaCoproductSerializer
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.Event.{Created, FileCreated}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import shapeless.{:+:, CNil}

import scala.util.Try

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("@type")

  private implicit def refEncoder: Encoder[Ref] = Encoder.encodeString.contramap(_.iri.show)
  private implicit def refDecoder: Decoder[Ref] = absoluteIriDecoder.map(Ref(_))

  private implicit def pathEncoder: Encoder[Path] = Encoder.encodeString.contramap(_.toString)
  private implicit def pathDecoder: Decoder[Path] = Decoder.decodeString.emapTry(p => Try(Paths.get(p)))

  private implicit def fileAttributesEncoder: Encoder[FileAttributes] = deriveEncoder[FileAttributes]
  private implicit def fileAttributesDecoder: Decoder[FileAttributes] = deriveDecoder[FileAttributes]

  private implicit val encodeResId: Encoder[ResId] =
    Encoder.forProduct2("project", "id")(r => (r.parent.id, r.value.show))

  private implicit val decodeResId: Decoder[ResId] =
    Decoder.forProduct2("project", "id") { (projRef: ProjectRef, id: AbsoluteIri) =>
      Id(projRef, id)
    }

  implicit def eventEncoder(implicit iamClientConfig: IamClientConfig): Encoder[Event] = {
    val enc = deriveEncoder[Event]
    Encoder.instance { ev =>
      val json = enc(ev).removeKeys("id") deepMerge ev.id.asJson
      ev match {
        case _: FileCreated | _: Created => json deepMerge Json.obj("rev" -> Json.fromLong(1L))
        case _                           => json
      }
    }
  }

  private val dec = deriveDecoder[Event]

  implicit val eventDecoder: Decoder[Event] =
    Decoder.instance { hc =>
      val json = hc.value
      for {
        key <- json.as[ResId]
        combined = json deepMerge Json.obj("id" -> key.asJson)
        event <- dec(combined.hcursor)
      } yield event
    }

  class EventSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

    private implicit val appConfig = Settings(system).appConfig

    private val serializer = new AkkaCoproductSerializer[Event :+: CNil](1050)

    override final def manifest(o: AnyRef): String = serializer.manifest(o)

    override final def toBinary(o: AnyRef): Array[Byte] = serializer.toBinary(o)

    override final def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = serializer.fromBinary(bytes, manifest)

    override val identifier = serializer.identifier

  }
}
