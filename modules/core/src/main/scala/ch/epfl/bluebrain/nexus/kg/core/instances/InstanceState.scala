package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import io.circe.Json

/**
  * Enumeration type for possible states of an instance.
  */
sealed trait InstanceState extends Product with Serializable

object InstanceState {

  /**
    * Initial state for all instances.
    */
  final case object Initial extends InstanceState

  /**
    * State used for all instances that have been created and later possibly updated or deprecated.
    *
    * @param id         the unique identifier of the instance
    * @param rev        the selected revision number
    * @param value      the json representation of the instance
    * @param attachment the attachment information of the instance
    * @param deprecated the deprecation status
    */
  final case class Current(id: InstanceId,
                           rev: Long,
                           value: Json,
                           attachment: Option[Attachment.Meta] = None,
                           deprecated: Boolean)
      extends InstanceState
}
