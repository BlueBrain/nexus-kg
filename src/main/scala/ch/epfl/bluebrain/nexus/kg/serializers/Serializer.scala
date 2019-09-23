package ch.epfl.bluebrain.nexus.kg.serializers

import akka.actor.ExtendedActorSystem
import akka.http.scaladsl.model.Uri
import akka.serialization.SerializerWithStringManifest
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.serialization.AkkaCoproductSerializer
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.Event.{Created, FileCreated}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.storage.client.types.FileAttributes.{Digest => StorageDigest}
import ch.epfl.bluebrain.nexus.storage.client.types.{FileAttributes => StorageFileAttributes}
import io.circe.generic.extras.Configuration
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

  private implicit val refEncoder: Encoder[Ref] = Encoder.encodeString.contramap(_.iri.show)
  private implicit val refDecoder: Decoder[Ref] = absoluteIriDecoder.map(Ref(_))

  private implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.toString)
  private implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.emapTry(uri => Try(Uri(uri)))

  private implicit val uriPathEncoder: Encoder[Uri.Path] = Encoder.encodeString.contramap(_.toString)
  private implicit val uriPathDecoder: Decoder[Uri.Path] = Decoder.decodeString.emapTry(uri => Try(Uri.Path(uri)))

  private implicit val digestDecoder: Decoder[Digest] = deriveDecoder[Digest]
  private implicit val digestEncoder: Encoder[Digest] = deriveEncoder[Digest]

  private implicit val storageDigestEncoder: Encoder[StorageDigest] = deriveEncoder[StorageDigest]
  private implicit val storageFileAttributesEncoder: Encoder[StorageFileAttributes] =
    deriveEncoder[StorageFileAttributes]

  private implicit val fileAttributesEncoder: Encoder[FileAttributes] = deriveEncoder[FileAttributes]
  private implicit val fileAttributesDecoder: Decoder[FileAttributes] = deriveDecoder[FileAttributes]

  private implicit val storageReferenceEncoder: Encoder[StorageReference] = deriveEncoder[StorageReference]
  private implicit val storageReferenceDecoder: Decoder[StorageReference] = deriveDecoder[StorageReference]

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

  implicit val eventDecoder: Decoder[Event] = {
    val dec = deriveDecoder[Event]
    Decoder.instance { hc =>
      val json = hc.value
      for {
        key <- json.as[ResId]
        combined = json deepMerge Json.obj("id" -> key.asJson)
        event <- dec(combined.hcursor)
      } yield event
    }
  }

  class EventSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

    private implicit val appConfig: AppConfig = Settings(system).appConfig

    private val serializer = new AkkaCoproductSerializer[Event :+: CNil](1050)

    override final def manifest(o: AnyRef): String = serializer.manifest(o)

    override final def toBinary(o: AnyRef): Array[Byte] = serializer.toBinary(o)

    override final def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = serializer.fromBinary(bytes, manifest)

    override val identifier: Int = serializer.identifier

  }
}
