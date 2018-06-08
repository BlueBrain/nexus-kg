package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Clock

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources
import ch.epfl.bluebrain.nexus.kg.resources.Command.{Create, Deprecate, Update}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.UnexpectedState
import ch.epfl.bluebrain.nexus.kg.resources.Repo.Agg
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json

/**
  * Resource repository.
  *
  * @param agg          an aggregate instance for resources
  * @param clock        a clock used to record the instants when changes occur
  * @param toIdentifier a mapping from an id to a persistent id
  * @tparam F           the repository effect type
  */
class Repo[F[_]: Monad](agg: Agg[F], clock: Clock, toIdentifier: ResId => String) {

  /**
    * Creates a new resource.
    *
    * @param id       the id of the resource
    * @param schema   the schema that constrains the resource
    * @param types    the collection of known types of the resource
    * @param source   the source representation
    * @param identity the identity that generated the change
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId,
             schema: Ref,
             types: Set[AbsoluteIri],
             source: Json,
             identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, Create(id, 0L, schema, types, source, clock.instant(), identity))

  /**
    * Updates a resource.
    *
    * @param id       the id of the resource
    * @param rev      the last known revision of the resource
    * @param types    the new collection of known resource types
    * @param source   the source representation
    * @param identity the identity that generated the change
    * @return either a rejection or the new resource representation in the F context
    */
  def update(id: ResId,
             rev: Long,
             types: Set[AbsoluteIri],
             source: Json,
             identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, Update(id, rev, types, source, clock.instant(), identity))

  /**
    * Deprecates a resource.
    *
    * @param id       the id of the resource
    * @param rev      the last known revision of the resource
    * @param identity the identity that generated the change
    * @return either a rejection or the new resource representation in the F context
    */
  def deprecate(id: ResId, rev: Long, identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, Deprecate(id, rev, clock.instant(), identity))

  /**
    * Attempts to read the resource identified by the argument id.
    *
    * @param id the id of the resource
    * @return the optional resource in the F context
    */
  def get(id: ResId): OptionT[F, Resource] =
    OptionT(
      agg.currentState(toIdentifier(id)).map(_.asResource)
    )

  /**
    * Attempts the read the resource identified by the argument id at the argument revision.
    *
    * @param id  the id of the resource
    * @param rev the revision of the resource
    * @return the optional resource in the F context
    */
  def get(id: ResId, rev: Long): OptionT[F, Resource] =
    OptionT(
      agg
        .foldLeft[State](toIdentifier(id), State.Initial) {
          case (state, event) if event.rev <= rev => Repo.next(state, event)
          case (state, _)                         => state
        }
        .map(_.asResource.filter(_.rev == rev))
    )

  /**
    * Attempts to read the resource identified by the argument id at the revision identified by the argument tag. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id  the id of the resource.
    * @param tag the tag of the resource
    * @return the optional resource in the F context
    */
  def get(id: ResId, tag: String): OptionT[F, Resource] =
    for {
      resource <- get(id)
      rev      <- OptionT.fromOption[F](resource.tags.get(tag))
      value    <- get(id, rev)
    } yield value

  private def evaluate(id: ResId, cmd: Command): EitherT[F, Rejection, Resource] =
    for {
      result   <- EitherT(agg.eval(toIdentifier(id), cmd))
      resource <- result.resourceT(UnexpectedState(id.ref))
    } yield resource
}

object Repo {

  /**
    * Aggregate type for resources.
    *
    * @tparam F the effect type under which the aggregate operates
    */
  type Agg[F[_]] = Aggregate[F] {
    type Identifier = String
    type Command    = resources.Command
    type Event      = resources.Event
    type State      = resources.State
    type Rejection  = resources.Rejection
  }

  private[resources] final val initial: State                                             = State.Initial
  private[resources] final def next(state: State, ev: Event): State                       = ???
  private[resources] final def eval(state: State, cmd: Command): Either[Rejection, Event] = ???
}
