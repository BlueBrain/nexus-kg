package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID
import java.util.regex.Pattern

import cats.data.EitherT
import cats.implicits._
import cats.{Monad, MonadError, Show}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticSearchConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.{resultOrFailures, KgError}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import io.circe.parser._

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
    * Attempts to convert the current view to a labeled view when required. This conversion is only targetting ''AggregateElasticSearchView of ViewRef[ProjectRef]'',
    * returning all the other views unchanged.
    * For the case of ''AggregateElasticSearchView of ViewRef[ProjectRef]'', the conversion is successful when the the mapping ''projectRef -> projectLabel'' exists on the ''cache''
    */
  def labeled[F[_]](implicit projectCache: ProjectCache[F], F: Monad[F]): EitherT[F, Rejection, View] =
    this match {
      case AggregateElasticSearchViewRefs(v) =>
        val refToLabel = projectCache.getProjectLabels(v.projects)
        EitherT(refToLabel.map(
          resultOrFailures(_).bimap(LabelsNotFound, res => v.copy(value = v.value.map(vr => vr.map(res(vr.project)))))))
      case o => EitherT.rightT(o)
    }

  /**
    * Attempts to convert the current view to a referenced view when required. This conversion is only targetting ''AggregateElasticSearchView of ViewRef[ProjectLabel]'',
    * returning all the other views unchanged.
    * For the case of ''AggregateElasticSearchView of ViewRef[ProjectLabel]'',
    * the conversion is successful when the the mapping ''projectLabel -> projectRef'' and the viewId exists on the ''cache''
    */
  def referenced[F[_]](caller: Caller, acls: AccessControlLists)(implicit projectCache: ProjectCache[F],
                                                                 viewCache: ViewCache[F],
                                                                 F: Monad[F]): EitherT[F, Rejection, View] = {
    this match {
      case AggregateElasticSearchViewLabels(r) =>
        val labelIris = r.value.foldLeft(Map.empty[ProjectLabel, Set[AbsoluteIri]]) { (acc, c) =>
          acc + (c.project -> (acc.getOrElse(c.project, Set.empty) + c.id))
        }
        val projectsPerms = caller.hasPermission(acls, labelIris.keySet, query)
        val inaccessible  = labelIris.keySet -- projectsPerms
        if (inaccessible.nonEmpty) EitherT.leftT[F, View](ProjectsNotFound(inaccessible))
        else {
          val labelToRef = projectCache.getProjectRefs(r.projects)
          EitherT(labelToRef.map(resultOrFailures(_).left.map(ProjectsNotFound))).flatMap { projMap =>
            val view: View = r.copy(value = r.value.map(vr => vr.map(projMap(vr.project))))
            projMap.foldLeft(EitherT.rightT[F, Rejection](view)) {
              case (acc, (label, ref)) =>
                acc.flatMap { _ =>
                  EitherT(viewCache.get(ref).map { views =>
                    val toTarget = labelIris.getOrElse(label, Set.empty)
                    val found    = views.collect { case es: ElasticSearchView if toTarget.contains(es.id) => es.id }
                    (toTarget -- found).headOption.map(iri => NotFound(iri.ref)).toLeft(view)
                  })
                }
            }
          }
        }
      case o => EitherT.rightT(o)
    }
  }
}

object View {

  val query: Permission = Permission.unsafe("views/query")
  val write: Permission = Permission.unsafe("views/write")

  /**
    * Enumeration of single view types.
    */
  sealed trait SingleView extends View

  /**
    * Enumeration of multiple view types.
    */
  sealed trait AggregateView extends View

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param res a materialized resource
    * @return Right(view) if the resource is compatible with a View, Left(rejection) otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, View] = {
    val c          = res.value.graph.cursor()
    val uuidEither = c.downField(nxv.uuid).focus.as[UUID]

    def elasticSearch(): Either[Rejection, View] =
      // format: off
      for {
        uuid          <- uuidEither.toRejectionOnLeft(res.id.ref)
        mappingStr    <- c.downField(nxv.mapping).focus.as[String].toRejectionOnLeft(res.id.ref)
        mapping       <- parse(mappingStr).left.map[Rejection](_ => InvalidResourceFormat(res.id.ref, "mappings cannot be parsed into Json"))
        schemas       <- c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
        tag            = c.downField(nxv.resourceTag).focus.as[String].toOption
        includeMeta   <- c.downField(nxv.includeMetadata).focus.as[Boolean].orElse(false).toRejectionOnLeft(res.id.ref)
        sourceAsText  <- c.downField(nxv.sourceAsText).focus.as[Boolean].orElse(false).toRejectionOnLeft(res.id.ref)
      } yield
        ElasticSearchView(mapping, schemas, tag, includeMeta, sourceAsText, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
      // format: on

    def sparql(): Either[Rejection, View] =
      uuidEither
        .toRejectionOnLeft(res.id.ref)
        .map(uuid => SparqlView(res.id.parent, res.id.value, uuid, res.rev, res.deprecated))

    def multiEsView(): Either[Rejection, View] = {
      val id = res.id
      def viewRefs[A: NodeEncoder: Show](cursor: List[GraphCursor]): Either[Rejection, Set[ViewRef[A]]] =
        cursor.foldM(Set.empty[ViewRef[A]]) { (acc, blankC) =>
          for {
            project <- blankC.downField(nxv.project).focus.as[A].toRejectionOnLeft(res.id.ref)
            id      <- blankC.downField(nxv.viewId).focus.as[AbsoluteIri].toRejectionOnLeft(res.id.ref)
          } yield acc + ViewRef(project, id)
        }

      val result = for {
        uuid <- uuidEither.toRejectionOnLeft(res.id.ref)
        emptyViewRefs = Set.empty[ViewRef[String]]
      } yield AggregateElasticSearchView(emptyViewRefs, id.parent, uuid, id.value, res.rev, res.deprecated)

      val cursorList = c.downField(nxv.views).downArray.toList
      result.flatMap { v =>
        viewRefs[ProjectLabel](cursorList) match {
          case Right(projectLabels) => Right(v.copy(value = projectLabels))
          case Left(_)              => viewRefs[ProjectRef](cursorList).map(projectRefs => v.copy(value = projectRefs))
        }
      }
    }

    if (Set(nxv.View.value, nxv.Alpha.value, nxv.ElasticSearchView.value).subsetOf(res.types)) elasticSearch()
    else if (Set(nxv.View.value, nxv.SparqlView.value).subsetOf(res.types)) sparql()
    else if (Set(nxv.View.value, nxv.AggregateElasticSearchView.value).subsetOf(res.types)) multiEsView()
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
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
  final case class ElasticSearchView(
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
      * @param config the [[ElasticSearchConfig]]
      */
    def index(implicit config: ElasticSearchConfig): String = s"${config.indexPrefix}_$name"

    /**
      * Attempts to create the index for the [[ElasticSearchView]].
      *
      * @tparam F the effect type
      * @return ''Unit'' when the index was successfully created, a ''Rejection'' signaling the type of error
      *         when the index couldn't be created wrapped in an [[Either]]. The either is then wrapped in the
      *         effect type ''F''
      */
    def createIndex[F[_]](implicit elasticSearch: ElasticSearchClient[F],
                          config: ElasticSearchConfig,
                          F: MonadError[F, Throwable]): F[Unit] =
      elasticSearch
        .createIndex(index)
        .flatMap(_ => elasticSearch.updateMapping(index, config.docType, mapping))
        .flatMap {
          case true  => F.unit
          case false => F.raiseError(KgError.InternalError("View mapping validation could not be performed"))
        }
  }

  object ElasticSearchView {
    private val defaultViewId = UUID.fromString("684bd815-9273-46f4-ac1c-0383d4a98254")

    /**
      * Default [[ElasticSearchView]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef)(implicit elasticSearchConfig: ElasticSearchConfig): ElasticSearchView = {
      val mapping =
        jsonContentOf("/elasticsearch/mapping.json", Map(Pattern.quote("{{docType}}") -> elasticSearchConfig.docType))
      // format: off
      ElasticSearchView(mapping, Set.empty, None, includeMetadata = true, sourceAsText = true, ref, nxv.defaultElasticSearchIndex.value, defaultViewId, 1L, deprecated = false)
      // format: on
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

  object SparqlView {
    private val defaultViewId = UUID.fromString("d88b71d2-b8a4-4744-bf22-2d99ef5bd26b")

    /**
      * Default [[SparqlView]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef): SparqlView =
      SparqlView(ref, nxv.defaultSparqlIndex.value, defaultViewId, 1L, deprecated = false)

  }

  /**
    * Aggregation of [[ElasticSearchView]].
    *
    * @param value      the set of views that this view connects to when performing searches
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class AggregateElasticSearchView[P](
      value: Set[ViewRef[P]],
      ref: ProjectRef,
      uuid: UUID,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean
  ) extends AggregateView {
    def valueString(implicit P: Show[P]): Set[ViewRef[String]] = value.map(v => v.copy(project = v.project.show))

    def projects: Set[P] = value.map(_.project)

    def indices[F[_]](implicit projectCache: ProjectCache[F],
                      viewCache: ViewCache[F],
                      acls: AccessControlLists,
                      caller: Caller,
                      config: ElasticSearchConfig,
                      F: Monad[F]): F[Set[String]] =
      value.foldLeft(F.pure(Set.empty[String])) {
        case (accF, ViewRef(ref: ProjectRef, viewId)) =>
          (accF, viewCache.getBy[ElasticSearchView](ref, viewId), projectCache.getLabel(ref)).mapN {
            case (acc, Some(view), Some(label)) if caller.hasPermission(acls, label, query) =>
              acc + view.index
            case (acc, _, _) => acc
          }
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

  type AggregateElasticSearchViewLabels = AggregateElasticSearchView[ProjectLabel]
  object AggregateElasticSearchViewLabels {
    final def unapply(arg: AggregateElasticSearchView[_]): Option[AggregateElasticSearchViewLabels] =
      arg.value.toSeq match {
        case ViewRef(_: ProjectLabel, _) +: _ => Some(arg.asInstanceOf[AggregateElasticSearchViewLabels])
        case _                                => None
      }
  }

  type AggregateElasticSearchViewRefs = AggregateElasticSearchView[ProjectRef]
  object AggregateElasticSearchViewRefs {
    final def unapply(arg: AggregateElasticSearchView[_]): Option[AggregateElasticSearchViewRefs] =
      arg.value.toSeq match {
        case ViewRef(_: ProjectRef, _) +: _ => Some(arg.asInstanceOf[AggregateElasticSearchViewRefs])
        case _                              => None
      }
  }

}
