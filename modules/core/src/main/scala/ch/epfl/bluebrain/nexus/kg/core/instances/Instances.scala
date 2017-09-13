package ch.epfl.bluebrain.nexus.kg.core.instances

import java.util.UUID

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ShaclSchema, ShaclValidator}
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.Rejection
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceCommand._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceState.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.core.instances.Instances.InstanceAggregate
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaRejection, Schemas}
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json
import journal.Logger

/**
  *
  * @param agg             the aggregate definition
  * @param schemas         the schemas operations bundle
  * @param validator       shacl validator
  * @param inOutFileStream the operations on incoming and outgoing file
  * @param F               a MonadError typeclass instance for ''F[_]''
  * @param al              defines how to deal with attachment's location
  * @tparam F   the monadic effect type
  * @tparam In  a typeclass defining the incoming stream client -> service
  * @tparam Out a typeclass defining the outgoing stream service -> client
  */

final class Instances[F[_], In, Out](
  agg: InstanceAggregate[F],
  schemas: Schemas[F],
  validator: ShaclValidator[F],
  inOutFileStream: InOutFileStream[F, In, Out])(implicit F: MonadError[F, Throwable], al: AttachmentLocation[F]) {

  private val logger = Logger[this.type]

  private def validatePayload(schemaId: SchemaId, json: Json): F[Unit] = {
    schemas.fetch(schemaId).flatMap {
      case Some(schema) =>
        validator(ShaclSchema(schema.value), json).flatMap { report =>
          if (report.conforms) F.pure(())
          else F.raiseError(CommandRejected(ShapeConstraintViolations(report.result.map(_.reason))))
        }
      case None         => F.raiseError(CommandRejected(SchemaRejection.SchemaDoesNotExist))
    }
  }

  private def evaluate(cmd: InstanceCommand, intent: => String): F[Current] = {
    F.pure {
      logger.debug(s"$intent: evaluating command '$cmd'")
    } flatMap { _ =>
      agg.eval(cmd.id.show, cmd)
    } flatMap {
      case Left(rejection) =>
        logger.debug(s"$intent: command '$cmd' was rejected due to '$rejection'")
        F.raiseError(CommandRejected(rejection))
      // $COVERAGE-OFF$
      case Right(s@Initial) =>
        logger.error(s"$intent: command '$cmd' evaluation failed, received an '$s' state")
        F.raiseError(Unexpected(s"Unexpected Initial state as outcome of evaluating command '$cmd'"))
      // $COVERAGE-ON$
      case Right(state: Current) =>
        logger.debug(s"$intent: command '$cmd' evaluation succeeded, generated state: '$state'")
        F.pure(state)
    }
  }

  /**
    * Creates a new instance by generating a unique identifier for it.
    *
    * @param schemaId the unique identifier of the schema for which the instance is created
    * @param value    the json representation of the instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(schemaId: SchemaId, value: Json): F[InstanceRef] = {
    val id = InstanceId(schemaId, UUID.randomUUID().toString.toLowerCase)
    create(id, value)
  }

  /**
    * Creates a new instance.
    *
    * @param id    the unique identifier of the instance
    * @param value the json representation of the instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: InstanceId, value: Json): F[InstanceRef] =
    for {
      _ <- schemas.assertUnlocked(id.schemaId)
      _ <- validatePayload(id.schemaId, value)
      r <- evaluate(CreateInstance(id, value), "Create instance")
    } yield InstanceRef(id, r.rev)

  /**
    * Updates an existing instance with a new json representation.  Updates can be performed only if the instance is not
    * deprecated.
    *
    * @param id    the unique identifier of the instance
    * @param rev   the last known revision of the instance
    * @param value the json representation of the instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def update(id: InstanceId, rev: Long, value: Json): F[InstanceRef] =
    for {
      _ <- validatePayload(id.schemaId, value)
      r <- evaluate(UpdateInstance(id, rev, value), "Update instance")
    } yield InstanceRef(id, r.rev)

  /**
    * Deprecates an instance locking it for further changes.
    *
    * @param id  the unique identifier of the instance
    * @param rev the last known revision of the instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def deprecate(id: InstanceId, rev: Long): F[InstanceRef] =
    evaluate(DeprecateInstance(id, rev), "Deprecate instance")
      .map(r => InstanceRef(id, r.rev))

  /**
    * Queries the system for the instance identified by the argument id.  The (in)existence of the instance is
    * represented by the [[scala.Option]] type wrapped within the ''F[_]'' context.
    *
    * @param id the unique identifier of the instance
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.instances.Instance]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: InstanceId): F[Option[Instance]] =
    agg.currentState(id.show).map {
      case Initial                                        => None
      case Current(_, rev, value, attachment, deprecated) => Some(Instance(id, rev, value, attachment.map(m => m.info), deprecated))
    }

  /**
    * Queries the system for the instance identified by the argument id at the specified revision.  The (in)existence of
    * the instance or the requested revision is represented by the [[scala.Option]] type wrapped within the ''F[_]''
    * context.
    *
    * @param id the unique identifier of the instance
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.instances.Instance]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: InstanceId, rev: Long): F[Option[Instance]] =
    fetchCurrent(id, rev).map {
      case None    => None
      case Some(c) => Some(Instance(c.id, rev, c.value, c.attachment.map(m => m.info), c.deprecated))
    }

  private def fetchCurrent(id: InstanceId, rev: Long): F[Option[Current]] =
    agg.foldLeft[InstanceState](id.show, Initial) {
      case (state, ev) if ev.rev <= rev => Instances.next(state, ev)
      case (state, _)                   => state
    }.map {
      case c: Current if c.rev == rev => Some(c)
      case _                          => None
    }

  /**
    * Updates an existing instance's state adding attachment relative location and metadata to it.
    * It also stores the actual Data of the attachment in the file system.
    *
    * @param id          the unique identifier of the instance
    * @param rev         the last known revision of the instance
    * @param filename    the original filename of the file to be attached
    * @param contentType the content type of the file to be attached
    * @param source      a typeclass defining the type of Source as some kind of stream
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    **/
  def createAttachment(id: InstanceId, rev: Long, filename: String, contentType: String, source: In): F[InstanceRef] = {
    val inOutResult: F[Either[Rejection, Attachment.Meta]] =
      F.pure {
        logger.debug(s"Uploading file '$filename' with contentType '$contentType' to instance '$id'")
      } flatMap { _ =>
        agg.currentState(id.show)
          .map(Instances.eval(_, CreateInstanceAttachment(id, rev, Attachment.EmptyMeta)))
      } flatMap {
        case Left(r)  => F.pure(Left(r))
        case Right(_) =>
          inOutFileStream.toSink(id, rev, filename, contentType, source).map(Right(_))
      }

    inOutResult.flatMap {
      case Left(rejection) =>
        logger.error(s"Error upload file '$filename' with contentType '$contentType' to instance '$id'. Rejection '$rejection'")
        F.raiseError(CommandRejected(rejection))
      case Right(meta)     =>
        logger.debug(s"Uploaded file '$filename' with contentType '$contentType' to instance '$id'")
        evaluate(CreateInstanceAttachment(id, rev, meta), "Create instance attachment")
          .map(curr => InstanceRef(id, curr.rev, Some(meta.info)))
    }
  }

  /**
    * Attempts to fetch the instance's last revision attachment metadata and data in a streaming fashion.
    *
    * @param id the unique identifier of the instance
    * @return an optional Tuple of [[ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment.Info]]
    *         and typeclass Out wrapped in the abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]]
    *         wrapped within ''F[_]'' otherwise
    */
  def fetchAttachment(id: InstanceId): F[Option[(Attachment.Info, Out)]] = {
    agg.currentState(id.show).flatMap {
      case Initial                               => F.pure(None)
      case Current(_, _, _, None, _)             => F.pure(None)
      case Current(_, _, _, Some(attachment), _) => sourceFrom(attachment)
    }
  }

  /**
    * Attempts to fetch the instance's specific revision attachment metadata and data in a streaming fashion.
    *
    * @param id  the unique identifier of the instance
    * @param rev the specific revision of the instance to target
    * @return an optional Tuple of [[ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment.Info]]
    *         and typeclass Out wrapped in the abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]]
    *         wrapped within ''F[_]'' otherwise
    */
  def fetchAttachment(id: InstanceId, rev: Long): F[Option[(Attachment.Info, Out)]] = {
    fetchCurrent(id, rev).flatMap {
      case Some(Current(_, _, _, Some(attachment), _)) => sourceFrom(attachment)
      case _                                           => F.pure(None)
    }
  }

  private def sourceFrom(attachment: Attachment.Meta): F[Option[(Attachment.Info, Out)]] = {
    inOutFileStream.toSource(al.toAbsoluteURI(attachment.fileUri)).map(out => Some(attachment.info -> out))
  }

  /**
    * Removes an existing instance attachment metadata for the latest revision (not the file)
    *
    * @param id  the unique identifier of the instance
    * @param rev the last known revision of the instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRef]] instance wrapped in the abstract ''F[_]''
    *         type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    **/
  def removeAttachment(id: InstanceId, rev: Long): F[InstanceRef] =
    evaluate(RemoveInstanceAttachment(id, rev), "Remove instance attachment")
      .map(r => InstanceRef(id, r.rev))
}

object Instances {

  /**
    * Aggregate type definition for Instances.
    *
    * @tparam F the aggregate effect type
    */
  type InstanceAggregate[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event = InstanceEvent
    type State = InstanceState
    type Command = InstanceCommand
    type Rejection = InstanceRejection
  }

  /**
    * Constructs a new ''Instances'' instance that bundles operations that can be performed against instances using the
    * underlying persistence abstraction.
    *
    * @param agg             the aggregate definition
    * @param schemas         the schemas operations bundle
    * @param validator       shacl validator
    * @param inOutFileStream the operations on incoming and outgoing file
    * @param F               a MonadError typeclass instance for ''F[_]''
    *                        al
    * @tparam F   the monadic effect type
    * @tparam In  a typeclass defining the incoming stream client -> service
    * @tparam Out a typeclass defining the outgoing stream service -> client
    **/
  final def apply[F[_], In, Out](
    agg: InstanceAggregate[F],
    schemas: Schemas[F],
    validator: ShaclValidator[F],
    inOutFileStream: InOutFileStream[F, In, Out])(implicit F: MonadError[F, Throwable], al: AttachmentLocation[F]): Instances[F, In, Out] =
    new Instances[F, In, Out](agg, schemas, validator, inOutFileStream)

  /**
    * The initial state of an instance.
    */
  final val initial: InstanceState = Initial

  /**
    * State transition function for instances; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  final def next(state: InstanceState, event: InstanceEvent): InstanceState = (state, event) match {
    case (Initial, InstanceCreated(id, rev, value)) => Current(id, rev, value, deprecated = false)
    // $COVERAGE-OFF$
    case (Initial, _) => Initial
    // $COVERAGE-ON$
    case (c: Current, _) if c.deprecated                        => c
    case (c: Current, _: InstanceCreated)                       => c
    case (c: Current, InstanceUpdated(_, rev, value))           => c.copy(rev = rev, value = value)
    case (c: Current, InstanceDeprecated(_, rev))               => c.copy(rev = rev, deprecated = true)
    case (c: Current, InstanceAttachmentCreated(_, rev, value)) => c.copy(rev = rev, attachment = Some(value))
    case (c: Current, InstanceAttachmentRemoved(_, rev))        => c.copy(rev = rev, attachment = None)
  }

  /**
    * Command evaluation logic for instances; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  final def eval(state: InstanceState, cmd: InstanceCommand): Either[InstanceRejection, InstanceEvent] = {
    def createInstance(c: CreateInstance) = state match {
      case Initial    => Right(InstanceCreated(c.id, 1L, c.value))
      case _: Current => Left(InstanceAlreadyExists)
    }

    def updateInstance(c: UpdateInstance) = state match {
      case Initial                      => Left(InstanceDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(InstanceIsDeprecated)
      case s: Current                   => Right(InstanceUpdated(s.id, s.rev + 1, c.value))
    }

    def deprecateInstance(c: DeprecateInstance) = state match {
      case Initial                      => Left(InstanceDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(InstanceIsDeprecated)
      case s: Current                   => Right(InstanceDeprecated(s.id, s.rev + 1))
    }

    def createInstanceAttachment(c: CreateInstanceAttachment) = state match {
      case Initial                      => Left(InstanceDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(InstanceIsDeprecated)
      case s: Current                   => Right(InstanceAttachmentCreated(s.id, s.rev + 1, c.value))
    }

    def removeInstanceAttachment(c: RemoveInstanceAttachment) = state match {
      case Initial                      => Left(InstanceDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(InstanceIsDeprecated)
      case Current(_, _, _, None, _)    => Left(AttachmentNotFound)
      case s: Current                   => Right(InstanceAttachmentRemoved(s.id, s.rev + 1))
    }


    cmd match {
      case c: CreateInstance           => createInstance(c)
      case c: UpdateInstance           => updateInstance(c)
      case c: DeprecateInstance        => deprecateInstance(c)
      case c: CreateInstanceAttachment => createInstanceAttachment(c)
      case c: RemoveInstanceAttachment => removeInstanceAttachment(c)
    }
  }
}