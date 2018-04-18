package ch.epfl.bluebrain.nexus.kg.core.resources

import java.time.Clock

import cats.MonadError
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.types.Rejection
import ch.epfl.bluebrain.nexus.kg.core.access.Access._
import ch.epfl.bluebrain.nexus.kg.core.access.HasAccess
import ch.epfl.bluebrain.nexus.kg.core.rejections.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceRejection.ParentResourceIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.resources.Resources.Agg
import ch.epfl.bluebrain.nexus.kg.core.resources.State.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.{Attachment, AttachmentStore}
import ch.epfl.bluebrain.nexus.kg.core.resources.{Command => Cmd, Event => Ev, State => St}
import ch.epfl.bluebrain.nexus.kg.core.types.{CallerCtx, IdVersioned, Project}
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import com.github.ghik.silencer.silent
import journal.Logger

@SuppressWarnings(Array("UnusedMethodParameter"))
class Resources[F[_], Type <: ResourceType, In, Out](agg: Agg[F], project: Project)(
    implicit attachStore: AttachmentStore[F, In, Out],
    F: MonadError[F, Throwable],
    clock: Clock) {

  private val logger: Logger = Logger[this.type]

  /**
    * Certain validation to take place during creation operations.
    * TODO: Use the validator
    */
  @silent
  private def validateCreate(value: Payload): F[Unit] = F.pure(())

  /**
    * Certain validation to take place during update operations.
    * TODO: Use the validator
    */
  @silent
  private def validateUpdate(value: Payload): F[Unit] = F.pure(())

  private def projectUnlocked(): F[Unit] =
    if (!project.deprecated) F.pure(())
    else F.raiseError(CommandRejected(ParentResourceIsDeprecated))

  /**
    * Attempts to create a new resource instance.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema that this resource validates against
    * @param value  the payload of the resource
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: String, schema: String, value: Payload, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Create): F[IdVersioned] =
    for {
      _     <- projectUnlocked()
      _     <- validateCreate(value)
      state <- eval(Cmd.Create(reprId(id, schema), 1L, caller.meta, value, tags + project.id), s"Create resource '$id'")
    } yield IdVersioned(state.id, state.rev)

  /**
    * Attempts to replace a new resource instance.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema that this resource validates against
    * @param rev    the last known revision of the resource instance
    * @param value  the new payload of the resource
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def replace(id: String, schema: String, rev: Long, value: Payload, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Write): F[IdVersioned] =
    for {
      _ <- projectUnlocked()
      _ <- validateUpdate(value)
      state <- eval(Cmd.Replace(reprId(id, schema), rev, caller.meta, value, tags + project.id),
                    s"Update resource '$id'")
    } yield IdVersioned(state.id, state.rev)

  /**
    * Attempts to add an attachment to a resource.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema that this resource validates against
    * @param rev    the last known revision of the resource instance
    * @param source the attachment source
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def attach(id: String,
             schema: String,
             rev: Long,
             attach: Attachment.BinaryDescription,
             source: In,
             tags: Set[String] = Set.empty)(implicit caller: CallerCtx,
                                            @silent access: Type HasAccess Attach): F[IdVersioned] = {
    val repr = reprId(id, schema)
    logger.debug(s"Uploading file '${attach.filename}' with mediaType '${attach.mediaType}' to instance '$id'")
    val inOutResult: F[Either[Rejection, BinaryAttributes]] =
      for {
        _          <- projectUnlocked()
        rejOrEvent <- agg.checkEval(repr.persId, Cmd.AttachVerify(repr, rev, caller.meta, attach))
        rejOrBinary <- rejOrEvent match {
          case Some(r) => F.pure(Left(r))
          case None    => attachStore.save(project.id, attach, source).map(Right(_))
        }
      } yield rejOrBinary

    inOutResult flatMap {
      case Left(r) =>
        logger.error(
          s"Error upload file '${attach.filename}' with mediaType '${attach.mediaType}' to id '$id'. Rejection '$r'")
        F.raiseError(CommandRejected(r))
      case Right(at) =>
        logger.debug(s"Uploaded file '${at.filename}' with mediaType '${at.mediaType}' to instance '$id'")
        eval(Cmd.Attach(repr, rev, caller.meta, at, tags + project.id), s"Attach resource '$id'")
          .map(state => IdVersioned(state.id, state.rev))
    }
  }

  /**
    * Attempts to remove an attachment from a resource.
    *
    * @param id       the identifier of the resource
    * @param schema   the identifier of the schema that this resource validates against
    * @param rev      the last known revision of the resource instance
    * @param fileName the attachment original file name
    * @param tags     the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def unattach(id: String, schema: String, rev: Long, fileName: String, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Attach): F[IdVersioned] =
    for {
      _ <- projectUnlocked()
      state <- eval(Cmd.Unattach(reprId(id, schema), rev, caller.meta, fileName, tags + project.id),
                    s"Unattach resource '$id'")
    } yield IdVersioned(state.id, state.rev)

  /**
    * Attempts to add a tag to alias a resource's revision with a provided ''name''.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema that this resource validates against
    * @param rev    the revision to be aliased
    * @param name   the name of the alias for the revision ''rev''
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def tag(id: String, schema: String, rev: Long, name: String, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Write): F[IdVersioned] =
    eval(Cmd.Tag(reprId(id, schema), rev, caller.meta, name, tags + project.id), s"Tag resource '$id'")
      .map(state => IdVersioned(state.id, state.rev))

  /**
    * Attempts to deprecate a resource locking it for further changes and blocking any attempts to create instances conforming to its
    * definition.
    *
    * @param id     the identifier of the resource
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def deprecate(id: String, schema: String, rev: Long, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Write): F[IdVersioned] =
    for {
      _     <- projectUnlocked()
      state <- eval(Cmd.Deprecate(reprId(id, schema), rev, caller.meta, tags + project.id), s"Deprecate res '$id'")
    } yield IdVersioned(state.id, state.rev)

  /**
    * Attempts to updeprecate a resource unlocking it from further changes.
    *
    * @param id     the identifier of the resource
    * @param tags   the tags associated to this operation
    * @return a [[IdVersioned]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def undeprecate(id: String, schema: String, rev: Long, tags: Set[String] = Set.empty)(
      implicit caller: CallerCtx,
      @silent access: Type HasAccess Write): F[IdVersioned] =
    for {
      _     <- projectUnlocked()
      state <- eval(Cmd.Undeprecate(reprId(id, schema), rev, caller.meta, tags + project.id), s"Undeprecate res '$id'")
    } yield IdVersioned(state.id, state.rev)

  /**
    * Attempts to fetch the resource's last revision attachment metadata and data in a streaming fashion.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @param name   the attachment filename
    * @return an optional Tuple of [[Attachment.Info]]
    *         and typeclass Out wrapped in the abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]]
    *         wrapped within ''F[_]'' otherwise
    */
  @SuppressWarnings(Array("PartialFunctionInsteadOfMatch"))
  def fetchAttachment(id: String, schema: String, name: String)(
      implicit @silent access: Type HasAccess Read): F[Option[(BinaryAttributes, Out)]] =
    agg.currentState(reprId(id, schema).persId).flatMap {
      case Initial                                => F.pure(None)
      case Current(_, _, _, _, attachments, _, _) => findAttachment(attachments, name)
    }

  /**
    * Attempts to fetch the resource's  attachment metadata and data in a streaming fashion at the specified revision.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @param rev    the revision attempted to be fetched
    * @param name   the attachment filename
    * @return an optional Tuple of [[BinaryAttributes]]
    *         and typeclass Out wrapped in the abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]]
    *         wrapped within ''F[_]'' otherwise
    */
  @SuppressWarnings(Array("PartialFunctionInsteadOfMatch"))
  def fetchAttachment(id: String, schema: String, rev: Long, name: String)(
      implicit @silent access: Type HasAccess Read): F[Option[(BinaryAttributes, Out)]] =
    stateAt(reprId(id, schema), rev).flatMap {
      case Current(_, `rev`, _, _, attachments, _, _) => findAttachment(attachments, name)
      case _                                          => F.pure(None)
    }

  /**
    * Attempts to fetch the resource's attachment metadata and data in a streaming fashion at the specified tag.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @param tag    the tag name (linked to a revision) attempted to be fetched
    * @param name   the attachment filename
    * @return an optional Tuple of [[BinaryAttributes]]
    *         and typeclass Out wrapped in the abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]]
    *         wrapped within ''F[_]'' otherwise
    */
  @SuppressWarnings(Array("PartialFunctionInsteadOfMatch"))
  def fetchAttachment(id: String, schema: String, tag: String, name: String)(
      implicit @silent access: Type HasAccess Read): F[Option[(BinaryAttributes, Out)]] =
    agg.currentState(reprId(id, schema).persId).flatMap {
      case Initial => F.pure(None)
      case c: Current =>
        c.tags.get(tag) match {
          case None      => F.pure(None)
          case Some(rev) => fetchAttachment(id, schema, rev, name)
        }
    }

  private def findAttachment(attachments: Set[BinaryAttributes], name: String): F[Option[(BinaryAttributes, Out)]] =
    attachments.find(_.filename == name) match {
      case None     => F.pure(None)
      case Some(at) => attachStore.fetch(at).map(out => Some(at -> out))
    }

  /**
    * Queries the system for the latest revision of the resource.
    * The (in)existence of the resource is represented by the [[scala.Option]] type wrapped within the ''F[_]'' context.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @return an optional [[Resource]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: String, schema: String)(implicit @silent access: Type HasAccess Read): F[Option[Resource]] =
    agg.currentState(reprId(id, schema).persId).map {
      case Initial    => None
      case c: Current => Some(Resource(c.id, c.rev, c.value, c.attachments, c.deprecated))
    }

  /**
    * Queries the system for a specific ''revision'' of theresource.
    * The (in)existence of the resource is represented by the [[scala.Option]] type wrapped within the ''F[_]'' context.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @param rev    the revision attempted to be fetched
    * @return an optional [[Resource]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: String, schema: String, rev: Long)(implicit @silent access: Type HasAccess Read): F[Option[Resource]] =
    stateAt(reprId(id, schema), rev).map {
      case c: Current if c.rev == rev => Some(Resource(c.id, c.rev, c.value, c.attachments, c.deprecated))
      case _                          => None
    }

  /**
    * Queries the system for a specific ''revision'' of theresource.
    * The (in)existence of the resource is represented by the [[scala.Option]] type wrapped within the ''F[_]'' context.
    *
    * @param id     the identifier of the resource
    * @param schema the identifier of the schema
    * @param tag    the tag name (linked to a revision) attempted to be fetched
    * @return an optional [[Resource]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: String, schema: String, tag: String)(
      implicit @silent access: Type HasAccess Read): F[Option[Resource]] =
    agg.currentState(reprId(id, schema).persId).flatMap {
      case Initial => F.pure(None)
      case c: Current =>
        c.tags.get(tag) match {
          case None      => F.pure(None)
          case Some(rev) => fetch(id, schema, rev)
        }
    }

  private def stateAt(id: RepresentationId, rev: Long): F[St] =
    agg.foldLeft[St](id.persId, Initial) {
      case (state, ev) if ev.rev <= rev => St.next(state, ev)
      case (state, _)                   => state
    }

  private def eval(cmd: Cmd, intent: => String): F[Current] =
    F.pure {
      logger.debug(s"$intent: evaluating command '$cmd''")
    } flatMap { _ =>
      agg.eval(cmd.id.persId, cmd)
    } flatMap {
      case Left(rejection) =>
        logger.debug(s"$intent: command '$cmd' was rejected due to '$rejection'")
        F.raiseError(CommandRejected(rejection))
      // $COVERAGE-OFF$
      case Right(s @ Initial) =>
        logger.error(s"$intent: command '$cmd' evaluation failed, received an '$s' state")
        F.raiseError(Unexpected(s"Unexpected Initial state as outcome of evaluating command '$cmd'"))
      // $COVERAGE-ON$
      case Right(state: Current) =>
        logger.debug(s"$intent: command '$cmd' evaluation succeeded, generated state: '$state'")
        F.pure(state)
    }

  private def reprId(id: String, schema: String) = RepresentationId(project.id, id, schema)
}

object Resources {

  type Agg[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event      = Ev
    type State      = St
    type Command    = Cmd
    type Rejection  = ResourceRejection
  }

  class ApplyPartialResource[Type <: ResourceType] {
    final def apply[F[_], In, Out](agg: Agg[F], project: Project)(implicit attachStore: AttachmentStore[F, In, Out],
                                                                  F: MonadError[F, Throwable],
                                                                  clock: Clock): Resources[F, Type, In, Out] =
      new Resources(agg, project)
  }

  def apply[Type <: ResourceType]: ApplyPartialResource[Type] = new ApplyPartialResource[Type]
}
