package ch.epfl.bluebrain.nexus.kg.core.queries

import java.util.UUID

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.queries.Queries.QueryAggregate
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryCommand._
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryEvent._
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryRejection._
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryState._
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json
import journal.Logger

import scala.util.matching.Regex

/**
  * Bundles operations that can be performed against organizations using the underlying persistence abstraction.
  *
  * @param agg the aggregate definition
  * @param F   a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
final class Queries[F[_]](agg: QueryAggregate[F])(implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  /**
    * Creates a new query instance by generating a unique identifier for it.
    *
    * @param value the json value of the query
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.queries.QueryRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(value: Json)(implicit ctx: CallerCtx): F[QueryRef] =
    create(QueryId(UUID.randomUUID().toString.toLowerCase()), value)

  /**
    * Creates a new query instance.
    *
    * @param id    the unique identifier of the query
    * @param value the json value of the query
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.queries.QueryRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: QueryId, value: Json)(implicit ctx: CallerCtx): F[QueryRef] =
    for {
      _       <- validateId(id)
      current <- evaluate(CreateQuery(id, ctx.meta, value), "Create query")
    } yield QueryRef(id, current.rev)

  /**
    * Updates the selected query with a new json value.
    *
    * @param id    the unique identifier of the query
    * @param rev   the last known revision of the query instance
    * @param value the new json value for the query
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.queries.QueryRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def update(id: QueryId, rev: Long, value: Json)(implicit ctx: CallerCtx): F[QueryRef] =
    evaluate(UpdateQuery(id, rev, ctx.meta, value), "Update query")
      .map(current => QueryRef(id, current.rev))

  /**
    * Queries the system for a query instance matching the argument ''id''.
    *
    * @param id the unique identifier of the query
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.queries.Query]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: QueryId): F[Option[Query]] =
    agg.currentState(id.show).map {
      case QueryState.Initial                   => None
      case QueryState.Current(_, rev, _, value) => Some(Query(id, rev, value))
    }

  /**
    * Queries the system for the query identified by the argument id at the specified revision.  The (in)existence of
    * the query or the requested revision is represented by the [[scala.Option]] type wrapped within the ''F[_]''
    * context.
    *
    * @param id  the unique identifier of the query
    * @param rev the revision attempted to be fetched
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.queries.Query]] query wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: QueryId, rev: Long): F[Option[Query]] =
    stateAt(id, rev).map {
      case c: Current if c.rev == rev => Some(Query(c.id, rev, c.value))
      case _                          => None
    }

  private def stateAt(id: QueryId, rev: Long): F[QueryState] =
    agg
      .foldLeft[QueryState](id.show, Initial) {
        case (state, ev) if ev.rev <= rev => Queries.next(state, ev)
        case (state, _)                   => state
      }

  private def evaluate(cmd: QueryCommand, intent: => String): F[Current] = {
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

  private val regex: Regex =
    """[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}""".r

  private def validateId(id: QueryId): F[Unit] = {
    F.pure {
      logger.debug(s"Validating id '$id'")
      id.id
    } flatMap {
      case regex() =>
        logger.debug(s"Id validation for '$id' succeeded")
        F.pure(())
      case _ =>
        logger.debug(s"Id validation for '$id' failed, did not match regex '$regex'")
        F.raiseError(CommandRejected(InvalidQueryId(id.id)))
    }
  }
}
object Queries {

  /**
    * Aggregate type definition for Queries.
    *
    * @tparam F the aggregate effect type
    */
  type QueryAggregate[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event      = QueryEvent
    type State      = QueryState
    type Command    = QueryCommand
    type Rejection  = QueryRejection
  }

  /**
    * Constructs a new ''Queries'' instance that bundles operations that can be performed against queries
    * using the underlying persistence abstraction.
    *
    * @param agg the aggregate definition
    * @param F   a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](agg: QueryAggregate[F])(implicit F: MonadError[F, Throwable]): Queries[F] =
    new Queries[F](agg)(F)

  /**
    * The initial state of a query.
    */
  final val initial: QueryState = Initial

  /**
    * State transition function for queries; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  final def next(state: QueryState, event: QueryEvent): QueryState = (state, event) match {
    case (Initial, QueryCreated(id, rev, meta, value)) => Current(id, rev, meta, value)
    // $COVERAGE-OFF$
    case (Initial, _) => Initial
    // $COVERAGE-ON$
    case (c: Current, _: QueryCreated)                   => c
    case (c: Current, QueryUpdated(_, rev, meta, value)) => c.copy(rev = rev, meta = meta, value = value)
  }

  /**
    * Command evaluation logic for queries; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  final def eval(state: QueryState, cmd: QueryCommand): Either[QueryRejection, QueryEvent] = {
    def createQuery(c: CreateQuery) = state match {
      case Initial    => Right(QueryCreated(c.id, 1L, c.meta, c.value))
      case _: Current => Left(QueryAlreadyExists)
    }

    def updateQuery(c: UpdateQuery) = state match {
      case Initial                      => Left(QueryDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current                   => Right(QueryUpdated(s.id, s.rev + 1, c.meta, c.value))
    }

    cmd match {
      case c: CreateQuery => createQuery(c)
      case c: UpdateQuery => updateQuery(c)
    }
  }
}
