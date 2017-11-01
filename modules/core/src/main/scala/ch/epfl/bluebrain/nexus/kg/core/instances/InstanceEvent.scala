package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for all events that are emitted for instances.
  */
sealed trait InstanceEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the instance
    */
  def id: InstanceId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object InstanceEvent {

  /**
    * Evidence that a new instance has been created.
    *
    * @param id    the unique identifier of the instance
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the json representation of the instance
    */
  final case class InstanceCreated(id: InstanceId, rev: Long, meta: Meta, value: Json) extends InstanceEvent

  /**
    * Evidence that an instance has been updated.
    *
    * @param id    the unique identifier of the instance
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new json representation of the instance
    */
  final case class InstanceUpdated(id: InstanceId, rev: Long, meta: Meta, value: Json) extends InstanceEvent

  /**
    * Evidence that an instance has been deprecated.
    *
    * @param id   the unique identifier of the instance
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class InstanceDeprecated(id: InstanceId, rev: Long, meta: Meta) extends InstanceEvent

  /**
    * Evidence that a new instance attachment has been created.
    *
    * @param id    the unique identifier of the instance
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the meta information of the attachment
    */
  final case class InstanceAttachmentCreated(id: InstanceId, rev: Long, meta: Meta, value: Attachment.Meta)
      extends InstanceEvent

  /**
    * Evidence that an instance attachment has been removed.
    *
    * @param id   the unique identifier of the instance
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class InstanceAttachmentRemoved(id: InstanceId, rev: Long, meta: Meta) extends InstanceEvent

}
