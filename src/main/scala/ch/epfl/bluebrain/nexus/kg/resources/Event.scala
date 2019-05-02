package ch.epfl.bluebrain.nexus.kg.resources

import java.nio.file.Path
import java.time.Instant

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas.fileSchemaUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.syntax._
import io.circe.{Encoder, Json}

/**
  * Enumeration of resource event types.
  */
sealed trait Event extends Product with Serializable {

  /**
    * @return the resource identifier
    */
  def id: Id[ProjectRef]

  /**
    * @return the revision that this event generated
    */
  def rev: Long

  /**
    * @return the instant when this event was recorded
    */
  def instant: Instant

  /**
    * @return the subject which created this event
    */
  def subject: Subject
}

object Event {

  /**
    * A witness to a resource creation.
    *
    * @param id      the resource identifier
    * @param schema  the schema that was used to constrain the resource
    * @param types   the collection of known resource types
    * @param source  the source representation of the resource
    * @param instant the instant when this event was recorded
    * @param subject the identity which generated this event
    */
  final case class Created(
      id: Id[ProjectRef],
      schema: Ref,
      types: Set[AbsoluteIri],
      source: Json,
      instant: Instant,
      subject: Subject
  ) extends Event {

    /**
      * the revision that this event generated
      */
    val rev: Long = 1L
  }

  /**
    * A witness to a resource update.
    *
    * @param id      the resource identifier
    * @param rev     the revision that this event generated
    * @param types   the collection of new known resource types
    * @param source  the source representation of the new resource value
    * @param instant the instant when this event was recorded
    * @param subject the identity which generated this event
    */
  final case class Updated(
      id: Id[ProjectRef],
      rev: Long,
      types: Set[AbsoluteIri],
      source: Json,
      instant: Instant,
      subject: Subject
  ) extends Event

  /**
    * A witness to a resource deprecation.
    *
    * @param id      the resource identifier
    * @param rev     the revision that this event generated
    * @param types   the collection of new known resource types
    * @param instant the instant when this event was recorded
    * @param subject the identity which generated this event
    */
  final case class Deprecated(
      id: Id[ProjectRef],
      rev: Long,
      types: Set[AbsoluteIri],
      instant: Instant,
      subject: Subject
  ) extends Event

  /**
    * A witness to a resource tagging. This event creates an alias for a revision.
    *
    * @param id        the resource identifier
    * @param rev       the revision that this event generated
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @param instant   the instant when this event was recorded
    * @param subject   the identity which generated this event
    */
  final case class TagAdded(
      id: Id[ProjectRef],
      rev: Long,
      targetRev: Long,
      tag: String,
      instant: Instant,
      subject: Subject
  ) extends Event

  /**
    * A witness that a file resource has been created.
    *
    * @param id         the resource identifier
    * @param storage    the storage used to save the file
    * @param attributes the metadata of the file
    * @param instant    the instant when this event was recorded
    * @param subject    the identity which generated this event
    */
  final case class FileCreated(
      id: Id[ProjectRef],
      storage: Storage,
      attributes: FileAttributes,
      instant: Instant,
      subject: Subject
  ) extends Event {

    /**
      * the revision that this event generated
      */
    val rev: Long = 1L

    /**
      * the schema that has been used to constrain the resource
      */
    val schema: Ref = fileSchemaUri.ref

    /**
      * the collection of known resource types
      */
    val types: Set[AbsoluteIri] = Set(nxv.File.value)
  }

  /**
    * A witness that a file resource has been updated.
    *
    * @param id         the resource identifier
    * @param storage    the storage used to save the file
    * @param rev        the revision that this event generated
    * @param attributes the metadata of the file
    * @param instant    the instant when this event was recorded
    * @param subject    the identity which generated this event
    */
  final case class FileUpdated(
      id: Id[ProjectRef],
      storage: Storage,
      rev: Long,
      attributes: FileAttributes,
      instant: Instant,
      subject: Subject
  ) extends Event {

    /**
      * the collection of known resource types
      */
    val types: Set[AbsoluteIri] = Set(nxv.File.value)
  }

  object JsonLd {

    private implicit val config: Configuration = Configuration.default
      .withDiscriminator("@type")
      .copy(transformMemberNames = {
        case "id"         => "_resourceId"
        case "storage"    => "_storage"
        case "rev"        => "_rev"
        case "instant"    => "_instant"
        case "subject"    => "_subject"
        case "schema"     => "_constrainedBy"
        case "attributes" => "_attributes"
        case "source"     => "_source"
        case "types"      => "_types"
        case "bytes"      => "_bytes"
        case "digest"     => "_digest"
        case "algorithm"  => "_algorithm"
        case "value"      => "_value"
        case "filename"   => "_filename"
        case "mediaType"  => "_mediaType"
        case "location"   => "_location"
        case other        => other
      })

    private implicit val refEncoder: Encoder[Ref]          = Encoder.encodeJson.contramap(_.iri.asJson)
    private implicit val uriEncoder: Encoder[Uri]          = Encoder.encodeString.contramap(_.toString)
    private implicit val uriPathEncoder: Encoder[Uri.Path] = Encoder.encodeString.contramap(_.toString)
    private implicit val pathEncoder: Encoder[Path]        = Encoder.encodeString.contramap(_.toString)
    private implicit val permEncoder: Encoder[Permission]  = Encoder.encodeString.contramap(_.value)

    private implicit val digestEncoder: Encoder[Digest] = deriveEncoder[Digest]

    private implicit val s3SettingsEncoder: Encoder[S3Settings] =
      Encoder.forProduct2("endpoint", "region")(settings => (settings.endpoint, settings.region))

    private implicit val fileAttributesEncoder: Encoder[FileAttributes] =
      deriveEncoder[FileAttributes]
        .mapJsonObject(_.remove("path").remove("uuid"))

    private implicit val idEncoder: Encoder[Id[ProjectRef]] =
      Encoder.encodeJson.contramap(_.value.asJson)

    private implicit def subjectIdEncoder(implicit ic: IamClientConfig): Encoder[Subject] =
      Encoder.encodeJson.contramap(_.id.asJson)

    private implicit val storageEncoder: Encoder[Storage] = Encoder.instance { storage =>
      val enc = deriveEncoder[Storage]
      enc(storage)
        .removeKeys("ref", "_resourceId", "_algorithm", "deprecated", "_rev")
        .deepMerge(Json.obj(nxv.storageId.prefix -> storage.id.asJson))
    }

    implicit def eventsEventEncoder(implicit ic: IamClientConfig): Encoder[Event] = {
      val enc = deriveEncoder[Event]
      Encoder.encodeJson.contramap[Event] { ev =>
        enc(ev).addContext(resourceCtxUri) deepMerge Json.obj(nxv.projectUuid.prefix -> ev.id.parent.id.toString.asJson)
      }
    }
  }
}
