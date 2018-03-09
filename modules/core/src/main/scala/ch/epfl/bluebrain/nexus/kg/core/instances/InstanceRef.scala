package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.kg.core.Ref
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment

/**
  * Reference data type to a specific instance revision.
  *
  * @param id  the unique identifier of the instance
  * @param rev a revision identifier of the instance
  * @param attachment an optional metadata information associated to the attachment
  */
final case class InstanceRef(id: InstanceId, rev: Long, attachment: Option[Attachment.Info] = None)

object InstanceRef {
  final implicit def instanceRef(ref: InstanceRef): Ref[InstanceId] = Ref(ref.id, ref.rev)
}
