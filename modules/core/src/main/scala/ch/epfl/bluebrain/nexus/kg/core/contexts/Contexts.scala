package ch.epfl.bluebrain.nexus.kg.core.contexts

import cats.MonadError
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextCommand._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextState.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts.ContextAggregate
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.{DomainDoesNotExist, DomainIsDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection.{OrgDoesNotExist, OrgIsDeprecated}
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json
import journal.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * Bundles operations that can be performed against contexts using the underlying persistence abstraction.
  *
  * @param agg     the aggregate definition
  * @param doms    the domains operations bundle
  * @param baseUri the base uri of the service
  * @param F       a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
final class Contexts[F[_]](agg: ContextAggregate[F], doms: Domains[F], baseUri: String)(
    implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  private def validateId(id: ContextId): F[Unit] = {
    logger.debug(s"Validating id '$id'")
    val regex = ContextName.regex
    id.contextName.show match {
      case regex(_, _, _) =>
        logger.debug(s"Id validation for '$id' succeeded")
        F.pure(())
      case _ =>
        logger.debug(s"Id validation for '$id' failed, name '${id.contextName.show}' did not match regex '$regex'")
        F.raiseError(CommandRejected(InvalidContextId(id)))
    }
  }

  /**
    * Asserts the domain exists and allows modifications on children resources.
    *
    * @param id the id of the domain
    * @return () or the appropriate rejection in the ''F'' context
    */
  def assertUnlocked(id: ContextId): F[Unit] = {
    val result = for {
      _     <- doms.assertUnlocked(id.domainId)
      state <- agg.currentState(id.show)
    } yield state

    result flatMap {
      case Initial                    => F.raiseError(CommandRejected(ContextDoesNotExist))
      case c: Current if c.deprecated => F.raiseError(CommandRejected(ContextIsDeprecated))
      case c: Current if !c.published => F.raiseError(CommandRejected(ContextIsNotPublished))
      case _                          => F.pure(())
    }
  }

  private val shapeValidationFailure = CommandRejected(ShapeConstraintViolations(List("Illegal context format")))

  private def validateRefContext(str: String): F[Unit] = {
    val start = s"$baseUri/contexts/"
    if (!str.startsWith(start))
      F.raiseError(
        CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' is not managed in this platform"))))
    else {
      val remaining = str.substring(start.length)
      ContextId(remaining) match {
        case Success(id) =>
          assertUnlocked(id).recoverWith {
            case CommandRejected(ContextDoesNotExist | OrgDoesNotExist | DomainDoesNotExist) =>
              F.raiseError(CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' does not exist"))))
            case CommandRejected(ContextIsDeprecated) =>
              F.raiseError(CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' is deprecated"))))
            case CommandRejected(ContextIsNotPublished) =>
              F.raiseError(CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' is not published"))))
            case CommandRejected(DomainIsDeprecated | OrgIsDeprecated) =>
              F.raiseError(
                CommandRejected(IllegalImportsViolation(
                  Set(s"Referenced context '$str' cannot be imported due to its domain deprecation status"))))
          }
        case Failure(NonFatal(_)) =>
          F.raiseError(
            CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' is not managed in this platform"))))
      }
    }
  }

  private def validatePayload(json: Json): F[Unit] = {
    json.hcursor.get[Json]("@context") match {
      case Left(_)      => F.raiseError(shapeValidationFailure)
      case Right(value) => validateContextValue(value)
    }
  }

  private def validateContextValue(json: Json): F[Unit] = {
    (json.asString, json.asArray, json.asObject) match {
      case (Some(str), _, _) => validateRefContext(str)
      case (_, Some(arr), _) => F.sequence(arr.map(j => validateContextValue(j))).map(_ => ())
      case (_, _, Some(_))   => F.pure(())
      case _                 => F.raiseError(shapeValidationFailure)
    }
  }

  private def evaluate(cmd: ContextCommand, intent: => String): F[Current] = {
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

  /**
    * Creates a new context instance.  The context id passed as an argument must contain an ''alphanumeric''  name with a
    * length between ''2'' and ''32'' characters (inclusive).
    *
    * @param id    the unique identifier of the context
    * @param value the json representation of the context
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(id: ContextId, value: Json)(implicit ctx: CallerCtx): F[ContextRef] =
    for {
      _ <- validateId(id)
      _ <- doms.assertUnlocked(id.domainId)
      _ <- validatePayload(value)
      r <- evaluate(CreateContext(id, ctx.meta, value), "Create context")
    } yield ContextRef(id, r.rev)

  /**
    * Updates an existing context instance with a new json representation.  Updates can be performed only if the context
    * instance is not in a published or deprecated status.
    *
    * @param id    the unique identifier of the context
    * @param rev   the last known revision of the context instance
    * @param value the json representation of the context
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def update(id: ContextId, rev: Long, value: Json)(implicit ctx: CallerCtx): F[ContextRef] =
    for {
      _ <- doms.assertUnlocked(id.domainId)
      _ <- validatePayload(value)
      r <- evaluate(UpdateContext(id, rev, ctx.meta, value), "Update context")
    } yield ContextRef(id, r.rev)

  /**
    * Publishes a context allowing instances conforming to its definition to be created.
    *
    * @param id  the unique identifier of the context
    * @param rev the last known revision of the context instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def publish(id: ContextId, rev: Long)(implicit ctx: CallerCtx): F[ContextRef] =
    for {
      _ <- doms.assertUnlocked(id.domainId)
      r <- evaluate(PublishContext(id, rev, ctx.meta), "Publish context")
    } yield ContextRef(id, r.rev)

  /**
    * Deprecates a context locking it for further changes and blocking any attempts to create instances conforming to its
    * definition.
    *
    * @param id  the unique identifier of the context
    * @param rev the last known revision of the context instance
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRef]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def deprecate(id: ContextId, rev: Long)(implicit ctx: CallerCtx): F[ContextRef] =
    evaluate(DeprecateContext(id, rev, ctx.meta), "Deprecate context")
      .map(r => ContextRef(id, r.rev))

  /**
    * Queries the system for the context identified by the argument id.  The (in)existence of the context is represented
    * by the [[scala.Option]] type wrapped within the ''F[_]'' context.
    *
    * @param id the unique identifier of the context
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.contexts.Context]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: ContextId): F[Option[Context]] =
    agg.currentState(id.show).map {
      case Initial                                          => None
      case Current(_, rev, _, value, published, deprecated) => Some(Context(id, rev, value, deprecated, published))
    }

  /**
    * Queries the system for the context identified by the argument id at the specified revision.  The (in)existence of
    * the context or the requested revision is represented by the [[scala.Option]] type wrapped within the ''F[_]''
    * context.
    *
    * @param id  the unique identifier of the context
    * @param rev the revision attempted to be fetched
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.contexts.Context]] schema wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: ContextId, rev: Long): F[Option[Context]] =
    stateAt(id, rev).map {
      case c: Current if c.rev == rev => Some(Context(c.id, rev, c.value, c.deprecated, c.published))
      case _                          => None
    }

  private def stateAt(id: ContextId, rev: Long): F[ContextState] =
    agg.foldLeft[ContextState](id.show, Initial) {
      case (state, ev) if ev.rev <= rev => Contexts.next(state, ev)
      case (state, _)                   => state
    }

  /**
    * Expands the argument context representation by recursively loading referenced contexts regardless of their status.
    *
    * @param context the context json representation
    * @return an expanded json context
    */
  def expand(context: Json): F[Json] = {
    def inner(ctx: Json): F[Json] =
      ctx.hcursor.get[Json]("@context") match {
        case Right(value) => expandValue(value)
        case Left(_)      => F.pure(Json.obj())
      }

    def expandValue(value: Json): F[Json] =
      (value.asString, value.asArray, value.asObject) match {
        case (Some(str), _, _) =>
          val start = s"$baseUri/contexts/"
          if (!str.startsWith(start))
            F.raiseError(
              CommandRejected(
                IllegalImportsViolation(Set(s"Referenced context '$str' is not managed in this platform"))))
          else {
            val remaining = str.substring(start.length)
            ContextId(remaining) match {
              case Success(id) =>
                fetch(id).flatMap {
                  case Some(ctx) => inner(ctx.value)
                  case None =>
                    F.raiseError(
                      CommandRejected(IllegalImportsViolation(Set(s"Referenced context '$str' does not exist"))))
                }
              case Failure(NonFatal(_)) =>
                F.raiseError(
                  CommandRejected(
                    IllegalImportsViolation(Set(s"Referenced context '$str' is not managed in this platform"))))
            }
          }
        case (_, Some(arr), _) =>
          F.sequence(arr.map(v => expandValue(v)))
            .map(values =>
              values.foldLeft(Json.obj()) { (acc, e) =>
                acc deepMerge e
            })
        case (_, _, Some(_)) => F.pure(value)
        case (_, _, _)       => F.raiseError(shapeValidationFailure)
      }

    inner(context).map(json => context deepMerge Json.obj("@context" -> json))
  }
}

object Contexts {

  /**
    * Aggregate type definition for Contexts.
    *
    * @tparam F the aggregate effect type
    */
  type ContextAggregate[F[_]] = Aggregate[F] {
    type Identifier = String
    type Event      = ContextEvent
    type State      = ContextState
    type Command    = ContextCommand
    type Rejection  = ContextRejection
  }

  /**
    * Constructs a new ''Contexts'' instance that bundles operations that can be performed against contexts using the
    * underlying persistence abstraction.
    *
    * @param agg     the aggregate definition
    * @param doms    the domains operations bundle
    * @param baseUri the base uri of the service
    * @param F       a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](agg: ContextAggregate[F], doms: Domains[F], baseUri: String)(
      implicit F: MonadError[F, Throwable]): Contexts[F] =
    new Contexts[F](agg, doms, baseUri)

  /**
    * The initial state of a context.
    */
  final val initial: ContextState = Initial

  /**
    * State transition function for contexts; considering a current state (the ''state'' argument) and an emitted
    * ''event'' it computes the next state.
    *
    * @param state the current state
    * @param event the emitted event
    * @return the next state
    */
  final def next(state: ContextState, event: ContextEvent): ContextState = (state, event) match {
    case (Initial, ContextCreated(id, rev, meta, value)) =>
      Current(id, rev, meta, value, published = false, deprecated = false)
    // $COVERAGE-OFF$
    case (Initial, _) => Initial
    // $COVERAGE-ON$
    case (c: Current, ContextDeprecated(_, rev, meta)) if c.published =>
      c.copy(rev = rev, meta = meta, deprecated = true)
    case (c: Current, _) if c.deprecated || c.published    => c
    case (c: Current, _: ContextCreated)                   => c
    case (c: Current, ContextUpdated(_, rev, meta, value)) => c.copy(rev = rev, meta = meta, value = value)
    case (c: Current, ContextPublished(_, rev, meta))      => c.copy(rev = rev, meta = meta, published = true)
    case (c: Current, ContextDeprecated(_, rev, meta))     => c.copy(rev = rev, meta = meta, deprecated = true)
  }

  /**
    * Command evaluation logic for contexts; considering a current ''state'' and a command to be evaluated either
    * reject the command or emit a new event that characterizes the change for an aggregate.
    *
    * @param state the current state
    * @param cmd   the command to be evaluated
    * @return either a rejection or emit an event
    */
  final def eval(state: ContextState, cmd: ContextCommand): Either[ContextRejection, ContextEvent] = {
    def createContext(c: CreateContext) = state match {
      case Initial    => Right(ContextCreated(c.id, 1L, c.meta, c.value))
      case _: Current => Left(ContextAlreadyExists)
    }

    def updateContext(c: UpdateContext) = state match {
      case Initial                      => Left(ContextDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(ContextIsDeprecated)
      case s: Current if s.published    => Left(CannotUpdatePublished)
      case s: Current                   => Right(ContextUpdated(s.id, s.rev + 1, c.meta, c.value))
    }

    def publishContext(c: PublishContext) = state match {
      case Initial                      => Left(ContextDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(ContextIsDeprecated)
      case s: Current if s.published    => Left(CannotUpdatePublished)
      case s: Current                   => Right(ContextPublished(s.id, s.rev + 1, c.meta))
    }

    def deprecateContext(c: DeprecateContext) = state match {
      case Initial                      => Left(ContextDoesNotExist)
      case s: Current if s.rev != c.rev => Left(IncorrectRevisionProvided)
      case s: Current if s.deprecated   => Left(ContextIsDeprecated)
      case s: Current                   => Right(ContextDeprecated(s.id, s.rev + 1, c.meta))
    }

    cmd match {
      case c: CreateContext    => createContext(c)
      case c: UpdateContext    => updateContext(c)
      case c: PublishContext   => publishContext(c)
      case c: DeprecateContext => deprecateContext(c)
    }
  }
}
