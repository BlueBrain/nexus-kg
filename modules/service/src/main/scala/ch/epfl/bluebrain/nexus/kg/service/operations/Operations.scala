package ch.epfl.bluebrain.nexus.kg.service.operations

import cats.syntax.all._
import cats.{MonadError, Show}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.types.Rejection
import ch.epfl.bluebrain.nexus.kg.service.CallerCtx
import ch.epfl.bluebrain.nexus.kg.service.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.OperationsConfig
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceCommand._
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations._
import ch.epfl.bluebrain.nexus.kg.service.operations.ResourceState._
import ch.epfl.bluebrain.nexus.kg.service.refs.RevisionedRef
import ch.epfl.bluebrain.nexus.kg.service.types.{Named, Revisioned}
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import com.github.ghik.silencer.silent
import journal.Logger
import shapeless.{Typeable, the}

abstract class Operations[F[_], Id: Show: Typeable, V: Typeable, StoredResEvent <: Revisioned](
    agg: Agg[F, Id, StoredResEvent],
    logger: Logger)(implicit F: MonadError[F, Throwable],
                    f: StoredResEvent => ResourceEvent[Id],
                    config: OperationsConfig) {

  type Resource
  private val V  = the[Typeable[V]]
  private val Id = the[Typeable[Id]]

  @silent
  def validate(id: Id, value: V): F[Unit] =
    validate(id)

  def validate(id: Id): F[Unit] =
    id match {
      case namedId: Named =>
        if (namedId.name.length > config.nameMaxLength) {
          logger.debug(s"Validation of id '$id' failed. Id length is longer than ${config.nameMaxLength}")
          F.raiseError(CommandRejected(InvalidId(id)))
        } else {
          logger.debug(s"Validation of id '$id' succeeded")
          F.pure(())
        }
      case _ =>
        F.pure(())
    }

  def validateUnlocked(id: Id): F[Unit] =
    agg.currentState(id.show) flatMap {
      case Initial                   => F.raiseError(CommandRejected(ParentResourceDoesNotExists))
      case Current(_, _, _, _, true) => F.raiseError(CommandRejected(ResourceIsDeprecated))
      case _                         => F.pure(())
    }

  implicit def buildResource(c: Current[Id, V]): Resource

  def create(id: Id, value: V)(implicit ctx: CallerCtx): F[RevisionedRef[Id]] =
    for {
      _ <- validate(id, value)
      r <- evaluate(CreateResource(id, ctx.meta, value), s"Create resource with id $id")
    } yield RevisionedRef(id, r.rev)

  def update(id: Id, rev: Long, value: V)(implicit ctx: CallerCtx): F[RevisionedRef[Id]] =
    for {
      _ <- validate(id, value)
      r <- evaluate(UpdateResource(id, rev, ctx.meta, value), s"Update resource with id $id")
    } yield RevisionedRef(id, r.rev)

  def deprecate(id: Id, rev: Long)(implicit ctx: CallerCtx): F[RevisionedRef[Id]] =
    evaluate(DeprecateResource(id, rev, ctx.meta), "Deprecate resource").map(current => RevisionedRef(id, current.rev))

  def fetchValue(id: Id): F[Option[V]] =
    agg.currentState(id.show).flatMap {
      case Initial           => F.pure(None)
      case c: Current[Id, V] => cast(c).map(casted => Some(casted.value))
    }

  def fetch(id: Id): F[Option[Resource]] =
    agg.currentState(id.show).flatMap {
      case Initial           => F.pure(None)
      case c: Current[Id, V] => cast(c).map(Some(_))
    }

  def fetch(id: Id, rev: Long): F[Option[Resource]] =
    stateAt(id, rev).flatMap {
      case c: Current[Id, V] if c.rev == rev => cast(c).map(Some(_))
      case _                                 => F.pure(None)
    }

  private def cast(c: Current[Id, V]): F[Current[Id, V]] =
    ResourceState.cast[Id, V](c) match {
      case Some(_) =>
        F.pure(c)
      case None =>
        F.raiseError(Unexpected(
          s"Received an id '${c.id}' or a value '${c.value}' incompatible to the expected types of id ${Id.describe} or value '${V.describe}'"))
    }

  private def stateAt(id: Id, rev: Long): F[ResourceState] =
    agg.foldLeft[ResourceState](id.show, Initial) {
      case (state, ev) if ev.rev <= rev => next[Id, V, StoredResEvent](state, ev)
      case (state, _)                   => state
    }

  private def evaluate(cmd: ResourceCommand[Id], intent: => String): F[Current[Id, V]] =
    F.pure {
      logger.debug(s"$intent: evaluating command '$cmd'")
    } flatMap { _ =>
      agg.eval(cmd.id.show, cmd)
    } flatMap {
      case Left(rejection) =>
        logger.debug(s"$intent: command '$cmd' was rejected due to '$rejection'")
        F.raiseError(CommandRejected(rejection))
      // $COVERAGE-OFF$
      case Right(s @ Initial) =>
        logger.error(s"$intent: command '$cmd' evaluation failed, received an '$s' state")
        F.raiseError(Unexpected(s"Unexpected Initial state as outcome of evaluating command '$cmd'"))
      // $COVERAGE-ON$
      case Right(state: Current[Id, V]) =>
        logger.debug(s"$intent: command '$cmd' evaluation succeeded, generated state: '$state'")
        F.pure(state)
    }
}

object Operations {

  type Agg[F[_], Id, StoredResEvent] = Aggregate[F] {
    type Identifier = String
    type Event      = StoredResEvent
    type State      = ResourceState
    type Command    = ResourceCommand[Id]
    type Rejection  = ResourceRejection
  }

  trait ResourceCommand[Id] extends Product with Serializable {
    def id: Id
    def meta: Meta
  }

  object ResourceCommand {

    final case class CreateResource[Id, V](id: Id, meta: Meta, value: V)            extends ResourceCommand[Id]
    final case class UpdateResource[Id, V](id: Id, rev: Long, meta: Meta, value: V) extends ResourceCommand[Id]
    final case class DeprecateResource[Id, V](id: Id, rev: Long, meta: Meta)        extends ResourceCommand[Id]
  }

  sealed trait ResourceEvent[Id] extends Product with Serializable with Revisioned {
    def id: Id
    def meta: Meta
  }

  object ResourceEvent {
    final case class ResourceCreated[Id, V](id: Id, rev: Long, meta: Meta, value: V) extends ResourceEvent[Id]
    final case class ResourceUpdated[Id, V](id: Id, rev: Long, meta: Meta, value: V) extends ResourceEvent[Id]
    final case class ResourceDeprecated[Id](id: Id, rev: Long, meta: Meta)           extends ResourceEvent[Id]

  }

  trait ResourceRejection extends Rejection
  object ResourceRejection {
    final case class ShapeConstraintViolations[Id](violations: List[String]) extends ResourceRejection
    final case object ResourceAlreadyExists                                  extends ResourceRejection
    final case object UnexpectedCasting                                      extends ResourceRejection
    final case object ResourceDoesNotExists                                  extends ResourceRejection
    final case object ParentResourceDoesNotExists                            extends ResourceRejection
    final case object ResourceIsDeprecated                                   extends ResourceRejection
    final case object IncorrectRevisionProvided                              extends ResourceRejection
    final case class InvalidId[Id](id: Id)                                   extends ResourceRejection

  }
}
