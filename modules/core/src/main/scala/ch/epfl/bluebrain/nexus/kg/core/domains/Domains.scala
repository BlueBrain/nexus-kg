package ch.epfl.bluebrain.nexus.kg.core.domains

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainCommand.{CreateDomain, DeprecateDomain}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainState.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains.DomainAggregate
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import journal.Logger

/**
  * Bundles operations that can be performed against domains using the underlying persistence abstraction.
  *
  * @param agg  the aggregate definition
  * @param orgs the organizations operations bundle
  * @param F    a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
final class Domains[F[_]](agg: DomainAggregate[F], orgs: Organizations[F])(implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  private val idRegex = "[a-z0-9]{3,32}".r

  private def validateId(id: DomainId): F[Unit] = {
    F.pure {
      logger.debug(s"Validating id '$id'")
      id.id
    } flatMap {
      case idRegex() =>
        logger.debug(s"Id validation for '$id' succeeded")
        F.pure(())
      case _ =>
        logger.debug(s"Id validation for '$id' failed, did not match regex '$idRegex'")
        F.raiseError(CommandRejected(InvalidDomainId(id.id)))
    }
  }

  /**
    * Asserts the domain exists and allows modifications on children resources.
    *
    * @param id the id of the domain
    * @return () or the appropriate rejection in the ''F'' context
    */
  def assertUnlocked(id: DomainId): F[Unit] = {
    val result = for {
      _     <- orgs.assertUnlocked(id.orgId)
      state <- agg.currentState(id.show)
    } yield state

    result flatMap {
      case Initial                    => F.raiseError(CommandRejected(DomainDoesNotExist))
      case c: Current if c.deprecated => F.raiseError(CommandRejected(DomainIsDeprecated))
      case _                          => F.pure(())
    }
  }

  /**
    * Creates a new domain instance.  The domain id passed as an argument must be ''alphanumeric'' with a length between
    * ''3'' and ''32'' characters (inclusive).
    *
    * @param id          the unique identifier of the domain
    * @param description a description for the domain
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.domains.DomainRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: DomainId, description: String): F[DomainRef] =
    for {
      _       <- validateId(id)
      _       <- orgs.assertUnlocked(id.orgId)
      current <- evaluate(CreateDomain(id, description), "Create domain")
    } yield DomainRef(id, current.rev)

  /**
    * Deprecates the selected domain prohibiting creating new instances against it.
    *
    * @param id  the unique identifier of the domain
    * @param rev the last known revision of the domain instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.domains.DomainRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def deprecate(id: DomainId, rev: Long): F[DomainRef] =
    evaluate(DeprecateDomain(id, rev), "Deprecate domain")
      .map(current => DomainRef(id, current.rev))

  /**
    * Queries the system for a domain instance matching the argument ''id''.
    *
    * @param id the unique identifier of the domain
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.domains.Domain]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: DomainId): F[Option[Domain]] =
    agg.currentState(id.show).map {
      case DomainState.Initial                                  => None
      case DomainState.Current(_, rev, deprecated, description) => Some(Domain(id, rev, deprecated, description))
    }

  private def evaluate(cmd: DomainCommand, intent: => String): F[Current] = {
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
      case Right(state: Current) =>
        logger.debug(s"$intent: command '$cmd' evaluation succeeded, generated state: '$state'")
        F.pure(state)
    }
  }

}

object Domains {

  /**
    * Aggregate type definition for Domains.
    *
    * @tparam F the aggregate effect type
    */
  type DomainAggregate[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event      = DomainEvent
    type State      = DomainState
    type Command    = DomainCommand
    type Rejection  = DomainRejection
  }

  /**
    * Constructs a new ''Domains'' instance that bundles operations that can be performed against domains using the
    * underlying persistence abstraction.
    *
    * @param agg  the aggregate definition
    * @param orgs the organization operations bundle
    * @param F    a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](agg: DomainAggregate[F], orgs: Organizations[F])(
      implicit F: MonadError[F, Throwable]): Domains[F] =
    new Domains[F](agg, orgs)(F)

  /**
    * The initial state of a domain.
    */
  final val initial: DomainState = Initial

  /**
    * State transition function for domains; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  final def next(state: DomainState, event: DomainEvent): DomainState = (state, event) match {
    case (Initial, DomainCreated(id, rev, desc)) => Current(id, rev, deprecated = false, desc)
    // $COVERAGE-OFF$
    case (Initial, _) => Initial
    // $COVERAGE-ON$
    case (c: Current, _) if c.deprecated   => c
    case (c: Current, _: DomainCreated)    => c
    case (c: Current, e: DomainDeprecated) => c.copy(rev = e.rev, deprecated = true)
  }

  /**
    * Command evaluation logic for domains; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  final def eval(state: DomainState, cmd: DomainCommand): Either[DomainRejection, DomainEvent] = {
    def createDomain(c: CreateDomain) = state match {
      case Initial => Right(DomainCreated(c.id, 1L, c.description))
      case _       => Left(DomainAlreadyExists)
    }

    def deprecateDomain(c: DeprecateDomain) = state match {
      case Initial                      => Left(DomainDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(DomainAlreadyDeprecated)
      case s: Current                   => Right(DomainDeprecated(s.id, s.rev + 1))
    }

    cmd match {
      case c: CreateDomain    => createDomain(c)
      case c: DeprecateDomain => deprecateDomain(c)
    }
  }
}
