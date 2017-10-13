package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import io.circe.Json

/**
  * Enumeration type for commands that apply to instances.
  */
sealed trait InstanceCommand extends Product with Serializable {

  /**
    * @return the unique identifier of the instance for which this command will be evaluated
    */
  def id: InstanceId
}

object InstanceCommand {

  /**
    * Command that signals the intent to create a new instance.
    *
    * @param id    the unique identifier for the instance to be created
    * @param value the json representation of the instance
    */
  final case class CreateInstance(id: InstanceId, value: Json) extends InstanceCommand

  /**
    * Command that signals the intent to update an instance value.
    *
    * @param id    the unique identifier of the instance to be updated
    * @param rev   the last known revision of the instance
    * @param value the new json representation of the instance
    */
  final case class UpdateInstance(id: InstanceId, rev: Long, value: Json) extends InstanceCommand

  /**
    * Command that signals the intent to deprecate an instance.
    *
    * @param id  the unique identifier of the instance to be deprecated
    * @param rev the last known revision of the instance
    */
  final case class DeprecateInstance(id: InstanceId, rev: Long) extends InstanceCommand

  /**
    * Command that signals the intent to add a file to an instance.
    *
    * @param id    the unique identifier of the instance to be updated
    * @param rev   the last known revision of the instance
    * @param value the meta information of the attachment
    */
  final case class CreateInstanceAttachment(id: InstanceId, rev: Long, value: Attachment.Meta) extends InstanceCommand

  /**
    * Command that signals the intent to remove the attachment linked to an instance
    *
    * @param id  the unique identifier of the instance to be updated
    * @param rev the last known revision of the instance
    */
  final case class RemoveInstanceAttachment(id: InstanceId, rev: Long) extends InstanceCommand

}
