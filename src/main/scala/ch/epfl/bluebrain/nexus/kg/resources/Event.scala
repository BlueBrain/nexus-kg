package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
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
    * @return the identity which generated this event
    */
  def identity: Identity
}

object Event {

  /**
    * A witness to a resource creation.
    *
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param schema   the schema that was used to constrain the resource
    * @param types    the collection of known resource types
    * @param source   the source representation of the resource
    * @param instant  the instant when this event was recorded
    * @param identity the identity which generated this event
    */
  final case class Created(
      id: Id[ProjectRef],
      rev: Long,
      schema: Ref,
      types: Set[AbsoluteIri],
      source: Json,
      instant: Instant,
      identity: Identity
  ) extends Event

  /**
    * A witness to a resource update.
    *
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param types    the collection of new known resource types
    * @param source   the source representation of the new resource value
    * @param instant  the instant when this event was recorded
    * @param identity the identity which generated this event
    */
  final case class Updated(
      id: Id[ProjectRef],
      rev: Long,
      types: Set[AbsoluteIri],
      source: Json,
      instant: Instant,
      identity: Identity
  ) extends Event

  /**
    * A witness to a resource deprecation.
    *
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param instant  the instant when this event was recorded
    * @param identity the identity which generated this event
    */
  final case class Deprecated(
      id: Id[ProjectRef],
      rev: Long,
      instant: Instant,
      identity: Identity
  ) extends Event

  /**
    * A witness to a resource tagging. This event creates an alias for a revision.
    *
    * @param id        the resource identifier
    * @param rev       the revision that this event generated
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @param instant   the instant when this event was recorded
    * @param identity  the identity which generated this event
    */
  final case class TagAdded(
      id: Id[ProjectRef],
      rev: Long,
      targetRev: Long,
      tag: String,
      instant: Instant,
      identity: Identity
  ) extends Event

  /**
    * A witness that a resource's attachment has been added.
    *
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param value    the metadata of the attachment
    * @param instant  the instant when this event was recorded
    * @param identity the identity which generated this event
    */
  final case class AttachmentAdded(
      id: Id[ProjectRef],
      rev: Long,
      value: BinaryAttributes,
      instant: Instant,
      identity: Identity
  ) extends Event

  /**
    * A witness that a resource's attachment has been removed.
    *
    * @param id       the resource identifier
    * @param rev      the revision that this event generated
    * @param filename the filename of the attachment removed
    * @param instant  the instant when this event was recorded
    * @param identity the identity which generated this event
    */
  final case class AttachmentRemoved(
      id: Id[ProjectRef],
      rev: Long,
      filename: String,
      instant: Instant,
      identity: Identity
  ) extends Event
}
