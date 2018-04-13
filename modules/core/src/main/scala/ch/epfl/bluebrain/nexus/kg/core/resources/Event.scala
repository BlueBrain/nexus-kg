package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment

/**
  * Enumeration type for all events that are emitted for resources.
  */
sealed trait Event extends Product with Serializable {
  def id: Key
  def rev: Long
  def meta: Meta
  def tags: Set[String]
}

object Event {

  /**
    * Evidence that a resource has been created.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the payload of the resource
    * @param tags  the tags added to this event
    */
  final case class Created(id: Key, rev: Long, meta: Meta, value: Payload, tags: Set[String] = Set.empty) extends Event

  /**
    * Evidence that a resource's payload has been replaced.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the payload of the resource
    * @param tags  the tags added to this event
    */
  final case class Replaced(id: Key, rev: Long, meta: Meta, value: Payload, tags: Set[String] = Set.empty) extends Event

  /**
    * Evidence that a resource has been deprecated.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param tags  the tags added to this event
    */
  final case class Deprecated(id: Key, rev: Long, meta: Meta, tags: Set[String] = Set.empty) extends Event

  /**
    * Evidence that a resource has been un-deprecated.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param tags  the tags added to this event
    */
  final case class Undeprecated(id: Key, rev: Long, meta: Meta, tags: Set[String] = Set.empty) extends Event

  /**
    * Evidence that a resource has been tagged. This event does not increase the revision number,
    * but it just creates an alias for it.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision that is being aliased with the provided ''name''
    * @param meta  the metadata associated to this event
    * @param name  the name of the alias for the provided ''rev''
    * @param tags  the tags added to this event
    */
  final case class Tagged(id: Key, rev: Long, meta: Meta, name: String, tags: Set[String] = Set.empty) extends Event

  /**
    * Evidence that a resource's attachment has been added.
    *
    * @param id    the primary key that identifies the resource
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the metadata of the attachment
    * @param tags  the tags added to this event
    */
  final case class Attached(id: Key, rev: Long, meta: Meta, value: Attachment, tags: Set[String] = Set.empty)
      extends Event

  /**
    * Evidence that a resource's attachment has been removed.
    *
    * @param id   the primary key that identifies the resource
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    * @param name the name of the removed attachment
    * @param tags the tags added to this event
    */
  final case class Unattached(id: Key, rev: Long, meta: Meta, name: String, tags: Set[String] = Set.empty) extends Event
}
