package ch.epfl.bluebrain.nexus.kg.core.organizations

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgCommand._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgState._
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations.OrgAggregate
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json
import journal.Logger
import cats.syntax.show._
/**
  * Bundles operations that can be performed against organizations using the underlying persistence abstraction.
  *
  * @param agg the aggregate definition
  * @param F   a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
final class Organizations[F[_]](agg: OrgAggregate[F])(implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  /**
    * Asserts the organization exists and allows modifications on children resources.
    *
    * @param id the id of the organization
    * @return () or the appropriate rejection in the ''F'' context
    */
  def assertUnlocked(id: OrgId): F[Unit] =
    agg.currentState(id.show) flatMap {
      case Initial                    => F.raiseError(CommandRejected(OrgDoesNotExist))
      case c: Current if c.deprecated => F.raiseError(CommandRejected(OrgIsDeprecated))
      case _                          => F.pure(())
    }

  /**
    * Creates a new organization instance.  The id passed as an argument must be ''alphanumeric'' with a length between
    * ''3'' and ''5'' characters (inclusive).
    *
    * @param id    the unique identifier of the organization
    * @param value the json value of the organization
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: OrgId, value: Json): F[OrgRef] =
    for {
      _ <- validateId(id)
      current <- evaluate(CreateOrg(id, value), "Create organization")
    } yield OrgRef(id, current.rev)

  /**
    * Updates the selected organization with a new json value.
    *
    * @param id    the unique identifier of the organization
    * @param rev   the last known revision of the organization instance
    * @param value the new json value for the organization
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def update(id: OrgId, rev: Long, value: Json): F[OrgRef] =
    evaluate(UpdateOrg(id, rev, value), "Update organization")
      .map(current => OrgRef(id, current.rev))

  /**
    * Deprecates the selected organization prohibiting creating new domains or new instances against it.
    *
    * @param id  the unique identifier of the organization
    * @param rev the last known revision of the organization instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def deprecate(id: OrgId, rev: Long): F[OrgRef] =
    evaluate(DeprecateOrg(id, rev), "Deprecate organization")
      .map(current => OrgRef(id, current.rev))

  /**
    * Queries the system for an organization instance matching the argument ''id''.
    *
    * @param id the unique identifier of the organization
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.organizations.Organization]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: OrgId): F[Option[Organization]] =
    agg.currentState(id.show).map {
      case OrgState.Initial                            => None
      case OrgState.Current(_, rev, value, deprecated) => Some(Organization(id, rev, value, deprecated))
    }

  private def evaluate(cmd: OrgCommand, intent: => String): F[Current] = {
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

  private val idRegex = "[a-z0-9]{3,5}".r

  private def validateId(id: OrgId): F[Unit] = {
    F.pure {
      logger.debug(s"Validating id '$id'")
      id.id
    } flatMap {
      case idRegex() =>
        logger.debug(s"Id validation for '$id' succeeded")
        F.pure(())
      case _         =>
        logger.debug(s"Id validation for '$id' failed, did not match regex '$idRegex'")
        F.raiseError(CommandRejected(InvalidOrganizationId(id.id)))
    }
  }
}

object Organizations {

  /**
    * Aggregate type definition for Organizations.
    *
    * @tparam F the aggregate effect type
    */
  type OrgAggregate[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event = OrgEvent
    type State = OrgState
    type Command = OrgCommand
    type Rejection = OrgRejection
  }

  /**
    * Constructs a new ''Organizations'' instance that bundles operations that can be performed against organizations
    * using the underlying persistence abstraction.
    *
    * @param agg the aggregate definition
    * @param F   a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](agg: OrgAggregate[F])(implicit F: MonadError[F, Throwable]): Organizations[F] =
    new Organizations[F](agg)(F)

  /**
    * The initial state of an organization.
    */
  final val initial: OrgState = Initial

  /**
    * State transition function for organizations; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  final def next(state: OrgState, event: OrgEvent): OrgState = (state, event) match {
    case (Initial, OrgCreated(id, rev, value)) => Current(id, rev, value)
    // $COVERAGE-OFF$
    case (Initial, _) => Initial
    // $COVERAGE-ON$
    case (c: Current, _) if c.deprecated         => c
    case (c: Current, _: OrgCreated)             => c
    case (c: Current, OrgUpdated(_, rev, value)) => c.copy(rev = rev, value = value)
    case (c: Current, OrgDeprecated(_, rev))     => c.copy(rev = rev, deprecated = true)
  }

  /**
    * Command evaluation logic for organizations; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  final def eval(state: OrgState, cmd: OrgCommand): Either[OrgRejection, OrgEvent] = {
    def createOrg(c: CreateOrg) = state match {
      case Initial    => Right(OrgCreated(c.id, 1L, c.value))
      case _: Current => Left(OrgAlreadyExists)
    }

    def updateOrg(c: UpdateOrg) = state match {
      case Initial                      => Left(OrgDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(OrgIsDeprecated)
      case s: Current                   => Right(OrgUpdated(s.id, s.rev + 1, c.value))
    }

    def deprecateOrg(c: DeprecateOrg) = state match {
      case Initial                      => Left(OrgDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(OrgIsDeprecated)
      case s: Current                   => Right(OrgDeprecated(s.id, s.rev + 1))
    }

    cmd match {
      case c: CreateOrg    => createOrg(c)
      case c: UpdateOrg    => updateOrg(c)
      case c: DeprecateOrg => deprecateOrg(c)
    }
  }
}