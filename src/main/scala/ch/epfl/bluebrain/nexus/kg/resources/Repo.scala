package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Clock

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{nxv, _}
import ch.epfl.bluebrain.nexus.kg.resources
import ch.epfl.bluebrain.nexus.kg.resources.Command._
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.Repo.Agg
import ch.epfl.bluebrain.nexus.kg.resources.State.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryAttributes, BinaryDescription}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import io.circe.Json

/**
  * Resource repository.
  *
  * @param agg          an aggregate instance for resources
  * @param clock        a clock used to record the instants when changes occur
  * @param toIdentifier a mapping from an id to a persistent id
  * @tparam F the repository effect type
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
  def create(id: ResId, schema: Ref, types: Set[AbsoluteIri], source: Json)(
      implicit identity: Identity): EitherT[F, Rejection, Resource] =
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
  def update(id: ResId, rev: Long, types: Set[AbsoluteIri], source: Json)(
      implicit identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, Update(id, rev, types, source, clock.instant(), identity))

  /**
    * Deprecates a resource.
    *
    * @param id       the id of the resource
    * @param rev      the last known revision of the resource
    * @param identity the identity that generated the change
    * @return either a rejection or the new resource representation in the F context
    */
  def deprecate(id: ResId, rev: Long)(implicit identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, Deprecate(id, rev, clock.instant(), identity))

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @param identity  the identity that generated the change
    * @return either a rejection or the new resource representation in the F context
    */
  def tag(id: ResId, rev: Long, targetRev: Long, tag: String)(
      implicit identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, AddTag(id, rev, targetRev, tag, clock.instant(), identity))

  /**
    * Adds an attachment to a resource.
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param attach the attachment description metadata
    * @param source the source of the attachment
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def attach[In](id: ResId, rev: Long, attach: BinaryDescription, source: In)(
      implicit identity: Identity,
      store: AttachmentStore[F, In, _]): EitherT[F, Rejection, Resource] =
    get(id).toRight(NotFound(id.ref)).flatMap { res =>
      if (res.deprecated) EitherT.leftT(IsDeprecated(id.ref))
      else if (res.rev != rev) EitherT.leftT(IncorrectRev(id.ref, rev))
      else
        store.save(id, attach, source).flatMap { attr =>
          evaluate(id, AddAttachment(id, rev, attr, clock.instant(), identity))
        }
    }

  /**
    * Removes an attachment from a resource.
    *
    * @param id       the id of the resource
    * @param rev      the last known revision of the resource
    * @param filename the attachment filename
    * @return either a rejection or the new resource representation in the F context
    */
  def unattach(id: ResId, rev: Long, filename: String)(implicit identity: Identity): EitherT[F, Rejection, Resource] =
    evaluate(id, RemoveAttachment(id, rev, filename, clock.instant(), identity))

  /**
    * Attempts to stream the resource's attachment identified by the argument id and the filename.
    *
    * @param id       the id of the resource.
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[Out](id: ResId, filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    attachmentFromState(agg.currentState(toIdentifier(id)), filename)

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the revision and the filename.
    *
    * @param id       the id of the resource.
    * @param rev      the revision of the resource
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[Out](id: ResId, rev: Long, filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    attachmentFromState(getState(id, rev), filename)

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the tag and the filename. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id       the id of the resource.
    * @param tag      the tag of the resource
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[Out](id: ResId, tag: String, filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    OptionT(agg.currentState(toIdentifier(id)).map {
      case Initial    => None
      case c: Current => c.tags.get(tag)
    }).flatMap(rev => getAttachment(id, rev, filename))

  private def attachmentFromState[Out](state: F[State], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    OptionT(state.map {
      case Initial => None
      case c: Current =>
        c.attachments.find(_.filename == filename).flatMap(at => store.fetch(at).toOption.map(out => at -> out))
    })

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
    OptionT(getState(id, rev).map(_.asResource.filter(_.rev == rev)))

  private def getState(id: ResId, rev: Long): F[State] =
    agg
      .foldLeft[State](toIdentifier(id), State.Initial) {
        case (state, event) if event.rev <= rev => Repo.next(state, event)
        case (state, _)                         => state
      }

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

  private[resources] final val initial: State = State.Initial

  private[resources] final def next(state: State, ev: Event): State =
    (state, ev) match {
      case (Initial, Created(id, 1L, schema, types, value, instant, identity)) =>
        Current(id, 1L, types, false, Map.empty, Set.empty, instant, instant, identity, identity, schema, value)
      case (Initial, _) => Initial
      case (c: Current, TagAdded(_, rev, targetRev, name, instant, identity)) =>
        c.copy(rev = rev, tags = c.tags + (name -> targetRev), updated = instant, updatedBy = identity)
      case (c: Current, _) if c.deprecated => c
      case (c: Current, Deprecated(_, rev, instant, identity)) =>
        c.copy(rev = rev, updated = instant, updatedBy = identity, deprecated = true)
      case (c: Current, Updated(_, rev, types, value, instant, identity)) =>
        c.copy(rev = rev, types = types, source = value, updated = instant, updatedBy = identity)
      case (c: Current, AttachmentAdded(_, rev, attachment, instant, identity)) =>
        c.copy(rev = rev,
               attachments = c.attachments.filter(_.filename != attachment.filename) + attachment,
               updated = instant,
               updatedBy = identity)
      case (c: Current, AttachmentRemoved(_, rev, name, instant, identity)) =>
        c.copy(rev = rev,
               attachments = c.attachments.filter(_.filename != name),
               updated = instant,
               updatedBy = identity)
    }

  private[resources] final def eval(state: State, cmd: Command): Either[Rejection, Event] = {

    def create(c: Create): Either[Rejection, Created] =
      state match {
        case Initial => Right(Created(c.id, 1L, c.schema, c.types, c.source, c.instant, c.identity))
        case _       => Left(AlreadyExists(c.id.ref))
      }

    def update(c: Update): Either[Rejection, Updated] =
      state match {
        case Initial                              => Left(NotFound(c.id.ref))
        case s: Current if s.rev != c.rev         => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated           => Left(IsDeprecated(c.id.ref))
        case s: Current if forbiddenUpdates(s, c) => Left(UpdateSchemaTypes(c.id.ref))
        case s: Current                           => Right(Updated(s.id, s.rev + 1, c.types, c.source, c.instant, c.identity))
      }

    def attach(c: AddAttachment): Either[Rejection, AttachmentAdded] =
      state match {
        case Initial                      => Left(NotFound(c.id.ref))
        case s: Current if s.rev != c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated   => Left(IsDeprecated(c.id.ref))
        case s: Current                   => Right(AttachmentAdded(s.id, s.rev + 1, c.value, c.instant, c.identity))
      }

    def unattach(c: RemoveAttachment): Either[Rejection, AttachmentRemoved] =
      state match {
        case Initial                      => Left(NotFound(c.id.ref))
        case s: Current if s.rev != c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated   => Left(IsDeprecated(c.id.ref))
        case s: Current if !s.attachments.exists(_.filename == c.filename) =>
          Left(AttachmentNotFound(c.id.ref, c.filename))
        case s: Current => Right(AttachmentRemoved(s.id, s.rev + 1, c.filename, c.instant, c.identity))
      }

    def forbiddenUpdates(s: Current, c: Update): Boolean =
      (s.types.contains(nxv.Schema) && !c.types.contains(nxv.Schema)) || (!s.types.contains(nxv.Schema) && c.types
        .contains(nxv.Schema))

    def tag(c: AddTag): Either[Rejection, TagAdded] =
      state match {
        case Initial                     => Left(NotFound(c.id.ref))
        case s: Current if s.rev < c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated  => Left(IsDeprecated(c.id.ref))
        case s: Current                  => Right(TagAdded(s.id, s.rev + 1, c.targetRev, c.tag, c.instant, c.identity))
      }

    def deprecate(c: Deprecate): Either[Rejection, Deprecated] =
      state match {
        case Initial                     => Left(NotFound(c.id.ref))
        case s: Current if s.rev < c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated  => Left(IsDeprecated(c.id.ref))
        case s: Current                  => Right(Deprecated(s.id, s.rev + 1, c.instant, c.identity))
      }

    cmd match {
      case cmd: Create           => create(cmd)
      case cmd: Update           => update(cmd)
      case cmd: Deprecate        => deprecate(cmd)
      case cmd: AddTag           => tag(cmd)
      case cmd: AddAttachment    => attach(cmd)
      case cmd: RemoveAttachment => unattach(cmd)
    }
  }
}
