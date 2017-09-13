package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import io.circe.Json

/**
  * Data type representing the state of an instance.
  *
  * @param id         a unique identifier for the instance
  * @param rev        the selected revision for the instance
  * @param value      the current value of the instance
  * @param attachment the meta information of the attachment
  * @param deprecated the deprecation status of the instance
  */
final case class Instance(id: InstanceId, rev: Long, value: Json, attachment: Option[Attachment.Info] = None, deprecated: Boolean)
