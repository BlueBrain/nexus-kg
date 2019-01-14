package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import cats.data.EitherT
import cats.implicits._
import cats.{Monad, MonadError, Show}
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticFailure}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticViewRefs, _}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidPayload, NotFound, ProjectsNotFound, Unexpected}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.{DeprecatedId, RevisionedId}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import io.circe.Json
import io.circe.parser._

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Enumeration of view types.
  */
sealed trait View extends Product with Serializable {

  /**
    * @return a reference to the project that the view belongs to
    */
  def ref: ProjectRef

  /**
    * @return the user facing view id
    */
  def id: AbsoluteIri

  /**
    * @return the underlying uuid generated for this view
    */
  def uuid: UUID

  /**
    * @return the view revision
    */
  def rev: Long

  /**
    * @return the deprecation state of the view
    */
  def deprecated: Boolean

  /**
    * @return a generated name that uniquely identifies the view and its current revision
    */
  def name: String =
    s"${ref.id}_${uuid}_$rev"

  /**
    * Attempts to convert the current view to a labeled view when required. This conversion is only targetting ''AggregateElasticView of ViewRef[ProjectRef]'',
    * returning all the other views unchanged.
    * For the case of ''AggregateElasticView of ViewRef[ProjectRef]'', the conversion is successful when the the mapping ''projectRef -> projectLabel'' exists on the ''cache''
    */
  def labeled[F[_]](implicit cache: DistributedCache[F], F: Monad[F]): EitherT[F, Rejection, View] =
    this match {
      case AggregateElasticViewRefs(r) =>
        cache.projectLabels(r.projects).map(projMap => r.copy(value = r.value.map(vr => vr.map(projMap(vr.project)))))
      case o => EitherT.rightT(o)
    }

  /**
    * Attempts to convert the current view to a referenced view when required. This conversion is only targetting ''AggregateElasticView of ViewRef[ProjectLabel]'',
    * returning all the other views unchanged.
    * For the case of ''AggregateElasticView of ViewRef[ProjectLabel]'',
    * the conversion is successful when the the mapping ''projectLabel -> projectRef'' and the viewId exists on the ''cache''
    */
  def referenced[F[_]](caller: Caller, acls: AccessControlLists)(implicit cache: DistributedCache[F],
                                                                 F: Monad[F]): EitherT[F, Rejection, View] = {
    this match {
      case AggregateElasticViewLabels(r) =>
        val labelIris = r.value.foldLeft(Map.empty[ProjectLabel, Set[AbsoluteIri]]) { (acc, c) =>
          acc + (c.project -> (acc.getOrElse(c.project, Set.empty) + c.id))
        }
        val projectsPerms = caller.hasPermission(acls, labelIris.keySet, viewsRead)
        val inaccessible  = labelIris.keySet -- projectsPerms
        if (inaccessible.nonEmpty) EitherT.leftT[F, View](ProjectsNotFound(inaccessible))
        else
          cache.projectRefs(r.projects).flatMap { projMap =>
            val view: View = r.copy(value = r.value.map(vr => vr.map(projMap(vr.project))))
            projMap.foldLeft(EitherT.rightT[F, Rejection](view)) {
              case (acc, (lb, ref)) =>
                acc.flatMap { _ =>
                  EitherT(cache.views(ref).map { views =>
                    val toTarget = labelIris.getOrElse(lb, Set.empty)
                    val found    = views.collect { case es: ElasticView if toTarget.contains(es.id) => es.id }
                    (toTarget -- found).headOption.map(iri => NotFound(Ref(iri))).toLeft(view)
                  })
                }
            }
          }
      case o => EitherT.rightT(o)
    }
  }
}

object View {
  val viewsRead = Set(Permission.unsafe("views/read"))

  /**
    * Enumeration of single view types.
    */
  sealed trait SingleView extends View

  /**
    * Enumeration of multiple view types.
    */
  sealed trait AggregateView extends View

  private implicit class NodeEncoderResultSyntax[A](private val enc: NodeEncoder.EncoderResult[A]) extends AnyVal {
    def toInvalidPayloadEither(ref: Ref): Either[Rejection, A] =
      enc.left.map(err =>
        InvalidPayload(ref, s"The provided payload could not be mapped to a view due to '${err.message}'"))
  }

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param res a materialized resource
    * @return Right(view) if the resource is compatible with a View, Left(rejection) otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, View] = {
    val c          = res.value.graph.cursor(res.id.value)
    val uuidEither = c.downField(nxv.uuid).focus.as[UUID]

    def elastic(): Either[Rejection, View] =
      // format: off
      for {
        uuid          <- uuidEither.toInvalidPayloadEither(res.id.ref)
        mappingStr    <- c.downField(nxv.mapping).focus.as[String].toInvalidPayloadEither(res.id.ref)
        mapping       <- parse(mappingStr).left.map[Rejection](_ => InvalidPayload(res.id.ref, "mappings cannot be parsed into Json"))
        schemas       = c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].map(_.toSet).getOrElse(Set.empty)
        tag           = c.downField(nxv.resourceTag).focus.as[String].toOption
        includeMeta   = c.downField(nxv.includeMetadata).focus.as[Boolean].getOrElse(false)
        sourceAsText  = c.downField(nxv.sourceAsText).focus.as[Boolean].getOrElse(false)
      } yield
        ElasticView(mapping, schemas, tag, includeMeta, sourceAsText, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
      // format: on

    def sparql(): Either[Rejection, View] =
      uuidEither
        .toInvalidPayloadEither(res.id.ref)
        .map(uuid => SparqlView(res.id.parent, res.id.value, uuid, res.rev, res.deprecated))

    def multiEsView(): Either[Rejection, View] = {
      val id = res.id
      def viewRefs[A: NodeEncoder: Show](cursor: List[GraphCursor]): Either[Rejection, Set[ViewRef[A]]] =
        cursor.foldM(Set.empty[ViewRef[A]]) { (acc, blankC) =>
          for {
            project <- blankC.downField(nxv.project).focus.as[A].toInvalidPayloadEither(res.id.ref)
            id      <- blankC.downField(nxv.viewId).focus.as[AbsoluteIri].toInvalidPayloadEither(res.id.ref)
          } yield acc + ViewRef(project, id)
        }

      val result = for {
        uuid <- uuidEither.toInvalidPayloadEither(res.id.ref)
        emptyViewRefs = Set.empty[ViewRef[String]]
      } yield AggregateElasticView(emptyViewRefs, id.parent, uuid, id.value, res.rev, res.deprecated)

      val cursorList = c.downField(nxv.views).downArray.toList
      result.flatMap { v =>
        viewRefs[ProjectLabel](cursorList) match {
          case Right(projectLabels) => Right(v.copy(value = projectLabels))
          case Left(_)              => viewRefs[ProjectRef](cursorList).map(projectRefs => v.copy(value = projectRefs))
        }
      }
    }

    if (Set(nxv.View.value, nxv.Alpha.value, nxv.ElasticView.value).subsetOf(res.types)) elastic()
    else if (Set(nxv.View.value, nxv.SparqlView.value).subsetOf(res.types)) sparql()
    else if (Set(nxv.View.value, nxv.AggregateElasticView.value).subsetOf(res.types)) multiEsView()
    else Left(InvalidPayload(res.id.ref, "The provided @type do not match any of the view types"))
  }

  /**
    * ElasticSearch specific view.
    *
    * @param mapping         the ElasticSearch mapping for the index
    * @param resourceSchemas set of schemas absolute iris used in the view. Indexing will be triggered only for
    *                        resources validated against any of those schemas
    * @param resourceTag     an optional tag. When present, indexing will be triggered only by resources tagged with the specified tag
    * @param includeMetadata flag to include or exclude metadata on the indexed Document
    * @param sourceAsText    flag to include or exclude the source Json as a blob
    *                        (if true, it will be included in the field '_original_source')
    * @param ref             a reference to the project that the view belongs to
    * @param id              the user facing view id
    * @param uuid            the underlying uuid generated for this view
    * @param rev             the view revision
    * @param deprecated      the deprecation state of the view
    */
  final case class ElasticView(
      mapping: Json,
      resourceSchemas: Set[AbsoluteIri],
      resourceTag: Option[String],
      includeMetadata: Boolean,
      sourceAsText: Boolean,
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends SingleView {

    /**
      * Generates the elasticSearch index
      *
      * @param config the [[ElasticConfig]]
      */
    def index(implicit config: ElasticConfig): String = s"${config.indexPrefix}_$name"

    /**
      * Attempts to create the index for the [[ElasticView]].
      *
      * @tparam F the effect type
      * @return ''Unit'' when the index was successfully created, a ''Rejection'' signaling the type of error
      *         when the index couldn't be created wrapped in an [[Either]]. The either is then wrapped in the
      *         effect type ''F''
      */
    def createIndex[F[_]](implicit elastic: ElasticClient[F],
                          config: ElasticConfig,
                          F: MonadError[F, Throwable]): F[Either[Rejection, Unit]] =
      elastic
        .createIndex(index)
        .flatMap(_ => elastic.updateMapping(index, config.docType, mapping))
        .map[Either[Rejection, Unit]] {
          case true  => Right(())
          case false => Left(Unexpected("View mapping validation could not be performed"))
        }
        .recoverWith {
          case err: ElasticFailure => F.pure(Left(InvalidPayload(Ref(id), err.body)))
          case NonFatal(err) =>
            val msg = Try(err.getMessage).getOrElse("")
            F.pure(Left(Unexpected(s"View mapping validation could not be performed. Cause '$msg'")))
        }
  }

  /**
    * Sparql specific view.
    *
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class SparqlView(
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends SingleView

  /**
    * Aggregation of [[ElasticView]].
    *
    * @param value      the set of views that this view connects to when performing searches
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class AggregateElasticView[P: Show](
      value: Set[ViewRef[P]],
      ref: ProjectRef,
      uuid: UUID,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean
  ) extends AggregateView {
    val valueString: Set[ViewRef[String]] = value.map(v => v.copy(project = v.project.show))

    def projects: Set[P] = value.map(_.project)

    def indices[F[_]](implicit cache: DistributedCache[F],
                      acls: AccessControlLists,
                      caller: Caller,
                      config: ElasticConfig,
                      F: Monad[F]): F[Set[String]] =
      value.foldLeft(F.pure(Set.empty[String])) {
        case (accF, ViewRef(ref: ProjectRef, id)) =>
          for {
            acc   <- accF
            views <- cache.views(ref).map(_.collect { case v: ElasticView if v.ref == ref && v.id == id => v })
            lb    <- ref.toLabel(cache)
          } yield lb.filter(caller.hasPermission(acls, _, viewsRead)).map(_ => acc ++ views.map(_.index)).getOrElse(acc)
        case (accF, _) => accF
      }
  }

  /**
    * A view reference is a unique way to identify a view
    * @param project the project reference
    * @param id the view id
    * @tparam P the generic type of the project reference
    */
  final case class ViewRef[P](project: P, id: AbsoluteIri) {
    def map[A](a: A): ViewRef[A] = copy(project = a)
  }

  private[indexing] type AggregateElasticViewLabels = AggregateElasticView[ProjectLabel]
  object AggregateElasticViewLabels {
    final def unapply(arg: AggregateElasticView[_]): Option[AggregateElasticViewLabels] =
      arg.value.toSeq match {
        case (ViewRef(_: ProjectLabel, _)) +: _ => Some(arg.asInstanceOf[AggregateElasticViewLabels])
        case _                                  => None
      }
  }

  private[indexing] type AggregateElasticViewRefs = AggregateElasticView[ProjectRef]
  object AggregateElasticViewRefs {
    final def unapply(arg: AggregateElasticView[_]): Option[AggregateElasticViewRefs] =
      arg.value.toSeq match {
        case (ViewRef(_: ProjectRef, _)) +: _ => Some(arg.asInstanceOf[AggregateElasticViewRefs])
        case _                                => None
      }
  }

  final implicit val viewRevisionedId: RevisionedId[View] = RevisionedId(view => (view.id, view.rev))
  final implicit val viewDeprecatedId: DeprecatedId[View] = DeprecatedId(r => (r.id, r.deprecated))

}
