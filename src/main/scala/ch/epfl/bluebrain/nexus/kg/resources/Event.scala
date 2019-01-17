package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant

import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.config.Schemas.fileSchemaUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json

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
    * @param id      the resource identifier
    * @param value   the metadata of the file
    * @param instant the instant when this event was recorded
    * @param subject the identity which generated this event
    */
  final case class CreatedFile(
      id: Id[ProjectRef],
      value: FileAttributes,
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
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param value    the metadata of the file
    * @param instant  the instant when this event was recorded
    * @param subject the identity which generated this event
    */
  final case class UpdatedFile(
      id: Id[ProjectRef],
      rev: Long,
      value: FileAttributes,
      instant: Instant,
      subject: Subject
  ) extends Event {

    /**
      * the collection of known resource types
      */
    val types: Set[AbsoluteIri] = Set(nxv.File.value)
  }
}
