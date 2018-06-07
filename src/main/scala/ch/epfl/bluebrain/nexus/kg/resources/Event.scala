package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant

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
      schema: Ref[_],
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
}
