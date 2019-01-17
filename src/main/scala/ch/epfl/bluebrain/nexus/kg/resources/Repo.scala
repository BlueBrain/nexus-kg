package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.effect.{Effect, Timer}
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Command._
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.Repo.Agg
import ch.epfl.bluebrain.nexus.kg.resources.State.{Current, Initial}
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.{resources, uuid}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.sourcing.Aggregate
import ch.epfl.bluebrain.nexus.sourcing.akka.{AkkaAggregate, SourcingConfig}
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
    * @param id      the id of the resource
    * @param schema  the schema that constrains the resource
    * @param types   the collection of known types of the resource
    * @param source  the source representation
    * @param subject the subject that generated the change
    * @param instant an optionally provided operation instant
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, schema: Ref, types: Set[AbsoluteIri], source: Json, instant: Instant = clock.instant)(
      implicit subject: Subject): EitherT[F, Rejection, Resource] =
    evaluate(id, Create(id, schema, types, source, instant, subject))

  /**
    * Updates a resource.
    *
    * @param id      the id of the resource
    * @param rev     the last known revision of the resource
    * @param types   the new collection of known resource types
    * @param source  the source representation
    * @param subject the subject that generated the change
    * @param instant an optionally provided operation instant
    * @return either a rejection or the new resource representation in the F context
    */
  def update(id: ResId, rev: Long, types: Set[AbsoluteIri], source: Json, instant: Instant = clock.instant)(
      implicit subject: Subject): EitherT[F, Rejection, Resource] =
    evaluate(id, Update(id, rev, types, source, instant, subject))

  /**
    * Deprecates a resource.
    *
    * @param id      the id of the resource
    * @param rev     the last known revision of the resource
    * @param subject the subject that generated the change
    * @param instant an optionally provided operation instant
    * @return either a rejection or the new resource representation in the F context
    */
  def deprecate(id: ResId, rev: Long, instant: Instant = clock.instant)(
      implicit subject: Subject): EitherT[F, Rejection, Resource] =
    evaluate(id, Deprecate(id, rev, instant, subject))

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @param subject   the identity that generated the change
    * @param instant   an optionally provided operation instant
    * @return either a rejection or the new resource representation in the F context
    */
  def tag(id: ResId, rev: Long, targetRev: Long, tag: String, instant: Instant = clock.instant)(
      implicit subject: Subject): EitherT[F, Rejection, Resource] =
    evaluate(id, AddTag(id, rev, targetRev, tag, instant, subject))

  /**
    * Creates a file resource.
    *
    * @param id       the id of the resource
    * @param fileDesc the file description metadata
    * @param source   the source of the file
    * @param instant  an optionally provided operation instant
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def createFile[In](id: ResId, fileDesc: FileDescription, source: In, instant: Instant = clock.instant)(
      implicit subject: Subject,
      store: FileStore[F, In, _]): EitherT[F, Rejection, Resource] =
    store.save(id, fileDesc, source).flatMap(attr => evaluate(id, CreateFile(id, attr, instant, subject)))

  /**
    * Replaces a file resource.
    *
    * @param id       the id of the resource
    * @param rev      the optional last known revision of the resource
    * @param fileDesc the file description metadata
    * @param source   the source of the file
    * @param instant  an optionally provided operation instant
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def updateFile[In](id: ResId, rev: Long, fileDesc: FileDescription, source: In, instant: Instant = clock.instant)(
      implicit subject: Subject,
      store: FileStore[F, In, _]): EitherT[F, Rejection, Resource] =
    store.save(id, fileDesc, source).flatMap(attr => evaluate(id, UpdateFile(id, rev, attr, instant, subject)))

  /**
    * Attempts to stream the file resource identified by the argument id.
    *
    * @param id the id of the resource.
    * @tparam Out the type for the output streaming of the file
    * @return the optional streamed file in the F context
    */
  def getFile[Out](id: ResId)(implicit store: FileStore[F, _, Out]): OptionT[F, (FileAttributes, Out)] =
    get(id) subflatMap (_.file.flatMap(at => store.fetch(at).toOption.map(out => at -> out)))

  /**
    * Attempts to stream the file resource identified by the argument id and the revision.
    *
    * @param id       the id of the resource.
    * @param rev      the revision of the resource
    * @tparam Out the type for the output streaming of the file
    * @return the optional streamed file in the F context
    */
  def getFile[Out](id: ResId, rev: Long)(implicit store: FileStore[F, _, Out]): OptionT[F, (FileAttributes, Out)] =
    get(id, rev) subflatMap (_.file.flatMap(at => store.fetch(at).toOption.map(out => at -> out)))

  /**
    * Attempts to stream the file resource identified by the argument id and the tag. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id       the id of the resource.
    * @param tag      the tag of the resource
    * @tparam Out the type for the output streaming of the file
    * @return the optional streamed file in the F context
    */
  def getFile[Out](id: ResId, tag: String)(implicit store: FileStore[F, _, Out]): OptionT[F, (FileAttributes, Out)] =
    get(id, tag) subflatMap (_.file.flatMap(at => store.fetch(at).toOption.map(out => at -> out)))

  /**
    * Attempts to read the resource identified by the argument id.
    *
    * @param id the id of the resource
    * @return the optional resource in the F context
    */
  def get(id: ResId): OptionT[F, Resource] =
    OptionT(agg.currentState(toIdentifier(id)).map(_.asResource))

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
      result   <- EitherT(agg.evaluateS(toIdentifier(id), cmd))
      resource <- result.resourceT(UnexpectedState(id.ref))
    } yield resource
}

object Repo {

  /**
    * Aggregate type for resources.
    *
    * @tparam F the effect type under which the aggregate operates
    */
  type Agg[F[_]] = Aggregate[F, String, resources.Event, resources.State, resources.Command, resources.Rejection]

  final val initial: State = State.Initial

  final def next(state: State, ev: Event): State =
    (state, ev) match {
      case (Initial, e @ Created(id, schema, types, value, tm, ident)) =>
        Current(id, e.rev, types, false, Map.empty, None, tm, tm, ident, ident, schema, value)
      case (Initial, e @ CreatedFile(id, file, tm, ident)) =>
        Current(id, e.rev, e.types, false, Map.empty, Some(file), tm, tm, ident, ident, e.schema, Json.obj())
      case (Initial, _) => Initial
      case (c: Current, TagAdded(_, rev, targetRev, name, tm, ident)) =>
        c.copy(rev = rev, tags = c.tags + (name -> targetRev), updated = tm, updatedBy = ident)
      case (c: Current, _) if c.deprecated => c
      case (c: Current, Deprecated(_, rev, _, tm, ident)) =>
        c.copy(rev = rev, updated = tm, updatedBy = ident, deprecated = true)
      case (c: Current, Updated(_, rev, types, value, tm, ident)) =>
        c.copy(rev = rev, types = types, source = value, updated = tm, updatedBy = ident)
      case (c: Current, UpdatedFile(_, rev, file, tm, ident)) =>
        c.copy(rev = rev, file = Some(file), updated = tm, updatedBy = ident)
    }

  final def eval(state: State, cmd: Command): Either[Rejection, Event] = {
    val (types, source) = transform(state, cmd)

    def create(c: Create): Either[Rejection, Created] =
      state match {
        case _ if c.schema == fileSchemaUri.ref => Left(NotFileResource(c.id.ref))
        case Initial                            => Right(Created(c.id, c.schema, types, source, c.instant, c.subject))
        case _                                  => Left(AlreadyExists(c.id.ref))
      }

    def createFile(c: CreateFile): Either[Rejection, CreatedFile] =
      state match {
        case Initial => Right(CreatedFile(c.id, c.value, c.instant, c.subject))
        case _       => Left(AlreadyExists(c.id.ref))
      }

    def updateFile(c: UpdateFile): Either[Rejection, UpdatedFile] =
      state match {
        case Initial                      => Left(NotFound(c.id.ref))
        case s: Current if s.rev != c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated   => Left(IsDeprecated(c.id.ref))
        case s: Current if s.file.isEmpty => Left(NotFileResource(c.id.ref))
        case s: Current                   => Right(UpdatedFile(s.id, s.rev + 1, c.value, c.instant, c.subject))
      }

    def update(c: Update): Either[Rejection, Updated] =
      state match {
        case Initial                      => Left(NotFound(c.id.ref))
        case s: Current if s.rev != c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated   => Left(IsDeprecated(c.id.ref))
        case s: Current                   => Right(Updated(s.id, s.rev + 1, types, source, c.instant, c.subject))
      }

    def tag(c: AddTag): Either[Rejection, TagAdded] =
      state match {
        case Initial                           => Left(NotFound(c.id.ref))
        case s: Current if s.rev < c.rev       => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.rev < c.targetRev => Left(IncorrectRev(c.id.ref, c.targetRev))
        case s: Current if s.deprecated        => Left(IsDeprecated(c.id.ref))
        case s: Current                        => Right(TagAdded(s.id, s.rev + 1, c.targetRev, c.tag, c.instant, c.subject))
      }

    def deprecate(c: Deprecate): Either[Rejection, Deprecated] =
      state match {
        case Initial                     => Left(NotFound(c.id.ref))
        case s: Current if s.rev < c.rev => Left(IncorrectRev(c.id.ref, c.rev))
        case s: Current if s.deprecated  => Left(IsDeprecated(c.id.ref))
        case s: Current                  => Right(Deprecated(s.id, s.rev + 1, s.types, c.instant, c.subject))
      }

    cmd match {
      case cmd: Create     => create(cmd)
      case cmd: CreateFile => createFile(cmd)
      case cmd: UpdateFile => updateFile(cmd)
      case cmd: Update     => update(cmd)
      case cmd: Deprecate  => deprecate(cmd)
      case cmd: AddTag     => tag(cmd)
    }
  }

  /**
    * Apply types and Json transformations on Create and Update commands
    *
    * @param state the current state
    * @param c     the incoming command
    * @return the tuple with the transformed types and payload
    */
  private def transform(state: State, c: Command): (Set[AbsoluteIri], Json) = {

    def extractUuidFrom(source: Json): String =
      source.hcursor.get[String](nxv.uuid.prefix).getOrElse(uuid())

    def changeView(source: Json, uuid: String): Json = {
      val transformed = source deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuid)).addContext(viewCtxUri)
      transformed.hcursor.get[Json]("mapping") match {
        case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
        case _                      => transformed
      }
    }

    (state, c) match {
      //views
      case (Initial, Create(_, `viewRef`, types, source, _, _)) =>
        types + nxv.View -> changeView(source, uuid())
      case (s: Current, u: Update) if s.schema == viewRef =>
        u.types + nxv.View -> changeView(u.source, extractUuidFrom(u.source))
      //resolvers
      case (Initial, Create(_, `resolverRef`, types, source, _, _)) =>
        types + nxv.Resolver -> source.addContext(resolverCtxUri)
      case (s: Current, u: Update) if s.schema == resolverRef =>
        u.types + nxv.Resolver -> u.source.addContext(resolverCtxUri)
      //schemas
      case (Initial, Create(_, `shaclRef`, types, source, _, _)) => types + nxv.Schema   -> source.addContext(shaclCtxUri)
      case (s: Current, u: Update) if s.schema == shaclRef       => u.types + nxv.Schema -> u.source.addContext(shaclCtxUri)
      //other
      case (Initial, Create(_, _, types, source, _, _)) => types                  -> source
      case (_, u: Update)                               => u.types                -> u.source
      case (s: Current, _)                              => s.types                -> s.source
      case _                                            => Set.empty[AbsoluteIri] -> Json.obj()
    }
  }

  private def aggregate[F[_]: Effect: Timer](implicit as: ActorSystem,
                                             mt: ActorMaterializer,
                                             sourcing: SourcingConfig,
                                             F: Monad[F]): F[Agg[F]] =
    AkkaAggregate.sharded[F](
      "resources",
      initial,
      next,
      (st, cmd) => F.pure(eval(st, cmd)),
      sourcing.passivationStrategy(),
      sourcing.retryStrategy,
      sourcing.akkaSourcingConfig,
      sourcing.shards
    )

  final def apply[F[_]: Effect: Timer](implicit as: ActorSystem,
                                       mt: ActorMaterializer,
                                       sourcing: SourcingConfig,
                                       clock: Clock = Clock.systemUTC): F[Repo[F]] =
    aggregate[F].map(agg => new Repo[F](agg, clock, resId => s"${resId.parent.id}-${resId.value.show}"))

}
