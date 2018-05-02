package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.commons.types.Meta
import ch.epfl.bluebrain.nexus.kg.core.resources.Command._
import ch.epfl.bluebrain.nexus.kg.core.resources.Event._
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.FileStream.StoredSummary
import eu.timepit.refined.auto._

/**
  * Enumeration type for possible states of a resource.
  */
sealed trait State extends Product with Serializable

object State {

  /**
    * Initial state for all resources.
    */
  final case object Initial extends State

  /**
    * State used for all resources that have been created and later possibly updated or deprecated.
    *
    * @param id          the identifier of the resource
    * @param rev         the selected revision number
    * @param meta        the metadata associated to this resource
    * @param value       the payload of the resource
    * @param attachments the attachment's metadata of the resource
    * @param deprecated  the deprecation status
    * @param tags        the key-pairs of alias names to revision numbers
    */
  final case class Current(id: RepresentationId,
                           rev: Long,
                           meta: Meta,
                           value: Payload,
                           attachments: Set[BinaryAttributes],
                           deprecated: Boolean,
                           tags: Map[String, Long])
      extends State

  /**
    * State transition function for resources; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  def next(state: State, event: Event): State = {
    (state, event) match {
      case (Initial, Created(id, 1L, meta, value, _)) =>
        Current(id, 1L, meta, value, Set.empty, deprecated = false, Map.empty)
      case (Initial, _)                                   => Initial
      case (c: Current, Undeprecated(_, rev, meta, _))    => c.copy(rev = rev, meta = meta, deprecated = false)
      case (c: Current, Tagged(_, rev, meta, name, _))    => c.copy(tags = c.tags + (name -> rev), meta = meta)
      case (c: Current, _) if c.deprecated                => c
      case (c: Current, Deprecated(_, rev, meta, _))      => c.copy(rev = rev, meta = meta, deprecated = true)
      case (c: Current, Replaced(_, rev, meta, value, _)) => c.copy(rev = rev, meta = meta, value = value)
      case (c: Current, Attached(_, rev, meta, attachment, _)) =>
        c.copy(rev = rev,
               meta = meta,
               attachments = c.attachments.filter(_.filename != attachment.filename) + attachment)
      case (c: Current, Unattached(_, rev, meta, name, _)) =>
        c.copy(rev = rev, meta = meta, attachments = c.attachments.filter(_.filename != name))
    }
  }

  /**
    * Command evaluation logic for resources; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  def eval(state: State, cmd: Command): Either[ResourceRejection, Event] = {
    val fakeSummary = StoredSummary("fake/path", Size(value = 0L), Digest("None", ""))

    def create(c: Create): Either[ResourceRejection, Created] =
      state match {
        case Initial => Right(Created(c.id, 1L, c.meta, c.value, c.tags))
        case _       => Left(ResourceAlreadyExists)
      }
    def replace(c: Replace): Either[ResourceRejection, Replaced] =
      state match {
        case Initial                      => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
        case s: Current if s.deprecated   => Left(ResourceIsDeprecated)
        case s: Current                   => Right(Replaced(s.id, s.rev + 1, c.meta, c.value, c.tags))
      }
    def tag(c: Tag): Either[ResourceRejection, Tagged] =
      state match {
        case Initial                     => Left(ResourceDoesNotExists)
        case s: Current if s.rev < c.rev => Left(IncorrectRevisionProvided)
        case s: Current                  => Right(Tagged(s.id, c.rev, c.meta, c.name, c.tags))
      }

    def attach(c: Attach): Either[ResourceRejection, Attached] =
      state match {
        case Initial                      => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
        case s: Current if s.deprecated   => Left(ResourceIsDeprecated)
        case s: Current                   => Right(Attached(s.id, s.rev + 1, c.meta, c.value, c.tags))
      }

    def attachVerify(c: AttachVerify): Either[ResourceRejection, Attached] =
      state match {
        case Initial                      => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
        case s: Current if s.deprecated   => Left(ResourceIsDeprecated)
        case s: Current                   => Right(Attached(s.id, s.rev + 1, c.meta, c.value.process(fakeSummary), c.tags))
      }

    def unattach(c: Unattach): Either[ResourceRejection, Unattached] =
      state match {
        case Initial                                                       => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev                                  => Left(IncorrectRevisionProvided)
        case s: Current if s.deprecated                                    => Left(ResourceIsDeprecated)
        case s: Current if !s.attachments.exists(_.filename == c.filename) => Left(AttachmentDoesNotExists)
        case s: Current                                                    => Right(Unattached(s.id, s.rev + 1, c.meta, c.filename, c.tags))
      }

    def deprecate(c: Deprecate): Either[ResourceRejection, Deprecated] =
      state match {
        case Initial                      => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
        case s: Current if s.deprecated   => Left(ResourceIsDeprecated)
        case s: Current                   => Right(Deprecated(s.id, s.rev + 1, c.meta, c.tags))
      }

    def undeprecate(c: Undeprecate): Either[ResourceRejection, Undeprecated] =
      state match {
        case Initial                      => Left(ResourceDoesNotExists)
        case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
        case s: Current if !s.deprecated  => Left(ResourceIsNotDeprecated)
        case s: Current                   => Right(Undeprecated(s.id, s.rev + 1, c.meta, c.tags))
      }

    cmd match {
      case cmd: Create       => create(cmd)
      case cmd: Replace      => replace(cmd)
      case cmd: Deprecate    => deprecate(cmd)
      case cmd: Undeprecate  => undeprecate(cmd)
      case cmd: Tag          => tag(cmd)
      case cmd: Attach       => attach(cmd)
      case cmd: AttachVerify => attachVerify(cmd)
      case cmd: Unattach     => unattach(cmd)

    }
  }

}
