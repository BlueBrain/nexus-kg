package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{Processed, Unprocessed}

/**
  * Enumeration type for commands that apply to resources.
  */
sealed trait Command extends Product with Serializable {
  def id: RepresentationId
  def rev: Long
  def meta: Meta
  def tags: Set[String]
}

object Command {

  /**
    * Command that signals the intent to create a new resource.
    *
    * @param id    the identifier for the resource to be created
    * @param meta  the metadata associated to this command
    * @param value the payload of the resource
    * @param tags  the tags associated to this command
    */
  final case class Create(id: RepresentationId, rev: Long, meta: Meta, value: Payload, tags: Set[String] = Set.empty)
      extends Command

  /**
    * Command that signals the intent to replace the payload of a resource.
    *
    * @param id    the identifier for the resource to be replaced
    * @param rev   the last known revision of the resource
    * @param meta  the metadata associated to this command
    * @param value the payload of the resource
    * @param tags  the tags associated to this command
    */
  final case class Replace(id: RepresentationId, rev: Long, meta: Meta, value: Payload, tags: Set[String] = Set.empty)
      extends Command

  /**
    * Command that signals the intent to deprecate a resource.
    *
    * @param id    the identifier for the resource to be deprecated
    * @param rev   the last known revision of the resource
    * @param meta  the metadata associated to this command
    * @param tags  the tags associated to this command
    */
  final case class Deprecate(id: RepresentationId, rev: Long, meta: Meta, tags: Set[String] = Set.empty) extends Command

  /**
    * Command that signals the intent to un-deprecate a resource.
    *
    * @param id    the identifier for the resource to be undeprecated
    * @param rev   the last known revision of the resource
    * @param meta  the metadata associated to this command
    * @param tags  the tags associated to this command
    */
  final case class Undeprecate(id: RepresentationId, rev: Long, meta: Meta, tags: Set[String] = Set.empty)
      extends Command

  /**
    * Command that signals the intent to tag a resource.
    *
    * @param id    the identifier for the resource to be tagged
    * @param rev   the revision to be tagged with the provided ''name''
    * @param meta  the metadata associated to this command
    * @param name  the name of the tag
    * @param tags  the tags associated to this command
    */
  final case class Tag(id: RepresentationId, rev: Long, meta: Meta, name: String, tags: Set[String] = Set.empty)
      extends Command

  /**
    * Command that signals the intent to add an attachment to the resource.
    *
    * @param id    the identifier for the resource to be attached
    * @param rev   the last known revision of the resource
    * @param meta  the metadata associated to this command
    * @param value the metadata of the attachment
    * @param tags  the tags associated to this command
    */
  final case class Attach(id: RepresentationId, rev: Long, meta: Meta, value: Processed, tags: Set[String] = Set.empty)
      extends Command

  private[resources] final case class AttachUnprocessed(id: RepresentationId,
                                                  rev: Long,
                                                  meta: Meta,
                                                  value: Unprocessed,
                                                  tags: Set[String] = Set.empty)
      extends Command

  /**
    * Command that signals the intent to remove an attachment from the resource.
    *
    * @param id   the identifier for the resource to be un-attached
    * @param rev  the last known revision of the resource
    * @param meta the metadata associated to this command
    * @param name the name of the attachment to be removed
    * @param tags the tags associated to this command
    */
  final case class Unattach(id: RepresentationId, rev: Long, meta: Meta, name: String, tags: Set[String] = Set.empty)
      extends Command
}
