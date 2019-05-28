package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID
import java.util.regex.Pattern
import java.util.regex.Pattern.quote

import cats.data.EitherT
import cats.effect.Async
import cats.implicits._
import cats.{Monad, MonadError, Show}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.search.FromPagination
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{ElasticSearchConfig, SparqlConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
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
import shapeless.{TypeCase, Typeable}

import scala.util.Try

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
    * For the case of ''AggregateView of ViewRef[ProjectRef]'', the conversion is successful when the the mapping ''projectRef -> projectLabel'' exists on the ''cache''
    */
  def labeled[F[_]](implicit projectCache: ProjectCache[F], F: Monad[F]): EitherT[F, Rejection, View] =
    this match {
      case v: AggregateView[_] =>
        v.value match {
          case `Set[ViewRef[ProjectRef]]`(viewRefs) =>
            val projectRefs = viewRefs.map(_.project)
            EitherT(projectCache.getProjectLabels(projectRefs).map(resultOrFailures).map {
              case Right(res)     => Right(v.toValue(viewRefs.map { case ViewRef(ref, id) => ViewRef(res(ref), id) }))
              case Left(projects) => Left(LabelsNotFound(projects))
            })
          case _ => EitherT.rightT(v)
        }
      case o => EitherT.rightT(o)
    }

  /**
    * Attempts to convert the current view to a referenced view when required. This conversion is only targetting ''AggregateElasticSearchView of ViewRef[ProjectLabel]'',
    * returning all the other views unchanged.
    * For the case of ''AggregateView of ViewRef[ProjectLabel]'',
    * the conversion is successful when the the mapping ''projectLabel -> projectRef'' and the viewId exists on the ''cache''
    */
  def referenced[F[_]](caller: Caller, acls: AccessControlLists)(implicit projectCache: ProjectCache[F],
                                                                 viewCache: ViewCache[F],
                                                                 F: Monad[F]): EitherT[F, Rejection, View] =
    this match {
      case v: AggregateView[_] =>
        v.value match {
          case `Set[ViewRef[ProjectLabel]]`(viewLabels) =>
            val labelIris = viewLabels.foldLeft(Map.empty[ProjectLabel, Set[AbsoluteIri]]) { (acc, c) =>
              acc + (c.project -> (acc.getOrElse(c.project, Set.empty) + c.id))
            }
            val projectsPerms = caller.hasPermission(acls, labelIris.keySet, query)
            val inaccessible  = labelIris.keySet -- projectsPerms
            if (inaccessible.nonEmpty) EitherT.leftT[F, View](ProjectsNotFound(inaccessible))
            else {
              val labelToRef = projectCache.getProjectRefs(labelIris.keySet)
              EitherT(labelToRef.map(resultOrFailures(_).left.map(ProjectsNotFound))).flatMap { projMap =>
                val view: View = v.toValue(viewLabels.map { case ViewRef(label, id) => ViewRef(projMap(label), id) })
                projMap.foldLeft(EitherT.rightT[F, Rejection](view)) {
                  case (acc, (label, ref)) =>
                    acc.flatMap { _ =>
                      EitherT(viewCache.get(ref).map { views =>
                        val toTarget = labelIris.getOrElse(label, Set.empty)
                        val found = v match {
                          case _: AggregateElasticSearchView[_] =>
                            views.collect { case es: ElasticSearchView if toTarget.contains(es.id) => es.id }
                          case _: AggregateSparqlView[_] =>
                            views.collect { case sparql: SparqlView if toTarget.contains(sparql.id) => sparql.id }
                        }
                        (toTarget -- found).headOption.map(iri => NotFound(iri.ref)).toLeft(view)
                      })
                    }
                }
              }
            }
          case _ => EitherT.rightT(v)
        }
      case v => EitherT.rightT(v)
    }
}

object View {

  val query: Permission = Permission.unsafe("views/query")
  val write: Permission = Permission.unsafe("views/write")

  /**
    * Enumeration of single view types.
    */
  sealed trait SingleView extends View {

    /**
      * @return set of schemas iris used in the view. Indexing will be triggered only for resources validated against any of those schemas (when empty, all resources are indexed)
      */
    def resourceSchemas: Set[AbsoluteIri]

    /**
      * @return set of types iris used in the view. Indexing will be triggered only for resources containing any of those types (when empty, all resources are indexed)
      */
    def resourceTypes: Set[AbsoluteIri]
  }

  /**
    * Enumeration of multiple view types.
    */
  sealed trait AggregateView[P] extends View {

    /**
      * @return the set of views that this view connects to when performing searches
      */
    def value: Set[ViewRef[P]]

    @SuppressWarnings(Array("RepeatedCaseBody"))
    def toValue[PP](newValue: Set[ViewRef[PP]]): AggregateView[PP] = this match {
      case agg: AggregateElasticSearchView[_] => agg.copy(value = newValue)
      case agg: AggregateSparqlView[_]        => agg.copy(value = newValue)
    }

    /**
      * Return the views with ''views/query'' permissions that are not deprecated from the provided ''viewRefs''
      *
      * @param viewRefs the provided set of view references
      */
    def queryableViews[F[_]: Monad, T <: View: Typeable](viewRefs: Set[ViewRef[ProjectRef]])(
        implicit projectCache: ProjectCache[F],
        viewCache: ViewCache[F],
        caller: Caller,
        acls: AccessControlLists): F[Set[T]] =
      viewRefs.toList.foldM(Set.empty[T]) {
        case (acc, ViewRef(ref, id)) =>
          (viewCache.getBy[T](ref, id) -> projectCache.getLabel(ref)).mapN {
            case (Some(view), Some(label)) if !view.deprecated && caller.hasPermission(acls, label, query) => acc + view
            case _                                                                                         => acc
          }
      }
  }

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param res a materialized resource
    * @return Right(view) if the resource is compatible with a View, Left(rejection) otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, View] = {
    val c          = res.value.graph.cursor()
    val uuidResult = c.downField(nxv.uuid).focus.as[UUID]

    def elasticSearch(): Either[Rejection, View] =
      // format: off
      for {
        uuid          <- uuidResult.toRejectionOnLeft(res.id.ref)
        mappingStr    <- c.downField(nxv.mapping).focus.as[String].toRejectionOnLeft(res.id.ref)
        mapping       <- parse(mappingStr).left.map[Rejection](_ => InvalidResourceFormat(res.id.ref, "mappings cannot be parsed into Json"))
        schemas       <- c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
        types         <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
        tag            = c.downField(nxv.resourceTag).focus.as[String].toOption
        includeMeta   <- c.downField(nxv.includeMetadata).focus.as[Boolean].orElse(false).toRejectionOnLeft(res.id.ref)
        includeDep    <- c.downField(nxv.includeDeprecated).focus.as[Boolean].orElse(true).toRejectionOnLeft(res.id.ref)
        sourceAsText  <- c.downField(nxv.sourceAsText).focus.as[Boolean].orElse(false).toRejectionOnLeft(res.id.ref)
      } yield
        ElasticSearchView(mapping, schemas, types, tag, includeMeta, includeDep, sourceAsText, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
      // format: on

    def sparql(): Either[Rejection, View] =
      // format: off
      for {
        uuid          <- uuidResult.toRejectionOnLeft(res.id.ref)
        schemas       <- c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
        types         <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
        tag            = c.downField(nxv.resourceTag).focus.as[String].toOption
        includeMeta   <- c.downField(nxv.includeMetadata).focus.as[Boolean].orElse(false).toRejectionOnLeft(res.id.ref)
        includeDep    <- c.downField(nxv.includeDeprecated).focus.as[Boolean].orElse(true).toRejectionOnLeft(res.id.ref)
      } yield
        SparqlView(schemas, types, tag, includeMeta,includeDep, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
    // format: on

    def viewRefs[A: NodeEncoder: Show](cursor: List[GraphCursor]): Either[Rejection, Set[ViewRef[A]]] =
      cursor.foldM(Set.empty[ViewRef[A]]) { (acc, blankC) =>
        for {
          project <- blankC.downField(nxv.project).focus.as[A].toRejectionOnLeft(res.id.ref)
          id      <- blankC.downField(nxv.viewId).focus.as[AbsoluteIri].toRejectionOnLeft(res.id.ref)
        } yield acc + ViewRef(project, id)
      }

    def aggregatedEsView(): Either[Rejection, View] =
      uuidResult.toRejectionOnLeft(res.id.ref).flatMap { uuid =>
        val cursorList = c.downField(nxv.views).downArray.toList
        viewRefs[ProjectLabel](cursorList) match {
          case Right(labels) =>
            Right(AggregateElasticSearchView(labels, res.id.parent, uuid, res.id.value, res.rev, res.deprecated))
          case Left(_) =>
            viewRefs[ProjectRef](cursorList).map(refs =>
              AggregateElasticSearchView(refs, res.id.parent, uuid, res.id.value, res.rev, res.deprecated))
        }
      }

    def aggregatedSparqlView(): Either[Rejection, View] =
      uuidResult.toRejectionOnLeft(res.id.ref).flatMap { uuid =>
        val cursorList = c.downField(nxv.views).downArray.toList
        viewRefs[ProjectLabel](cursorList) match {
          case Right(labels) =>
            Right(AggregateSparqlView(labels, res.id.parent, uuid, res.id.value, res.rev, res.deprecated))
          case Left(_) =>
            viewRefs[ProjectRef](cursorList).map(refs =>
              AggregateSparqlView(refs, res.id.parent, uuid, res.id.value, res.rev, res.deprecated))
        }
      }

    if (Set(nxv.View.value, nxv.ElasticSearchView.value).subsetOf(res.types)) elasticSearch()
    else if (Set(nxv.View.value, nxv.SparqlView.value).subsetOf(res.types)) sparql()
    else if (Set(nxv.View.value, nxv.AggregateElasticSearchView.value).subsetOf(res.types)) aggregatedEsView()
    else if (Set(nxv.View.value, nxv.AggregateSparqlView.value).subsetOf(res.types)) aggregatedSparqlView()
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
  }

  /**
    * ElasticSearch specific view.
    *
    * @param mapping           the ElasticSearch mapping for the index
    * @param resourceSchemas   set of schemas iris used in the view. Indexing will be triggered only for resources validated against any of those schemas (when empty, all resources are indexed)
    * @param resourceTypes     set of types iris used in the view. Indexing will be triggered only for resources containing any of those types (when empty, all resources are indexed)
    * @param resourceTag       an optional tag. When present, indexing will be triggered only by resources tagged with the specified tag
    * @param includeMetadata   flag to include or exclude metadata on the indexed Document
    * @param includeDeprecated flag to include or exclude the deprecated resources on the indexed Document
    * @param sourceAsText      flag to include or exclude the source Json as a blob
    *                          (if true, it will be included in the field '_original_source')
    * @param ref               a reference to the project that the view belongs to
    * @param id                the user facing view id
    * @param uuid              the underlying uuid generated for this view
    * @param rev               the view revision
    * @param deprecated        the deprecation state of the view
    */
  final case class ElasticSearchView(
      mapping: Json,
      resourceSchemas: Set[AbsoluteIri],
      resourceTypes: Set[AbsoluteIri],
      resourceTag: Option[String],
      includeMetadata: Boolean,
      includeDeprecated: Boolean,
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
      ElasticSearchView(mapping, Set.empty, Set.empty, None, includeMetadata = true, includeDeprecated = true, sourceAsText = true, ref, nxv.defaultElasticSearchIndex.value, defaultViewId, 1L, deprecated = false)
      // format: on
    }
  }

  /**
    * Sparql specific view.
    *
    * @param resourceSchemas   set of schemas iris used in the view. Indexing will be triggered only for resources validated against any of those schemas (when empty, all resources are indexed)
    * @param resourceTypes     set of types iris used in the view. Indexing will be triggered only for resources containing any of those types (when empty, all resources are indexed)
    * @param resourceTag       an optional tag. When present, indexing will be triggered only by resources tagged with the specified tag
    * @param includeMetadata   flag to include or exclude metadata on the index
    * @param includeDeprecated flag to include or exclude the deprecated resources on the index
    * @param ref               a reference to the project that the view belongs to
    * @param id                the user facing view id
    * @param uuid              the underlying uuid generated for this view
    * @param rev               the view revision
    * @param deprecated        the deprecation state of the view
    */
  final case class SparqlView(
      resourceSchemas: Set[AbsoluteIri],
      resourceTypes: Set[AbsoluteIri],
      resourceTag: Option[String],
      includeMetadata: Boolean,
      includeDeprecated: Boolean,
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends SingleView {

    private val incomingQuery: String                = contentOf("/blazegraph/incoming.txt")
    private val outgoingIncludeExternalQuery: String = contentOf("/blazegraph/outgoing_include_external.txt")
    private val outgoingScopedQuery: String          = contentOf("/blazegraph/outgoing_scoped.txt")

    private def replace(query: String, id: AbsoluteIri, pagination: FromPagination): String =
      query
        .replaceAll(quote("{id}"), id.asString)
        .replaceAll(quote("{offset}"), pagination.from.toString)
        .replaceAll(quote("{size}"), pagination.size.toString)

    /**
      * Runs incoming query using the provided SparqlView index against the provided [[BlazegraphClient]] endpoint
      *
      * @param id         the resource id. The query will select the incomings that match this id
      * @param pagination the pagination for the query
      * @tparam F the effect type
      */
    def incoming[F[_]: Async](id: AbsoluteIri, pagination: FromPagination)(implicit client: BlazegraphClient[F],
                                                                           config: SparqlConfig): F[LinkResults] =
      client.copy(namespace = index).queryRaw(replace(incomingQuery, id, pagination)).map(toSparqlLinks)

    /**
      * Runs outgoing query using the provided SparqlView index against the provided [[BlazegraphClient]] endpoint
      *
      * @param id                   the resource id. The query will select the incomings that match this id
      * @param pagination           the pagination for the query
      * @param includeExternalLinks flag to decide whether or not to include external links (not Nexus managed) in the query result
      * @tparam F the effect type
      */
    def outgoing[F[_]: Async](id: AbsoluteIri, pagination: FromPagination, includeExternalLinks: Boolean)(
        implicit client: BlazegraphClient[F],
        config: SparqlConfig): F[LinkResults] =
      if (includeExternalLinks)
        client
          .copy(namespace = index)
          .queryRaw(replace(outgoingIncludeExternalQuery, id, pagination))
          .map(toSparqlLinks)
      else client.copy(namespace = index).queryRaw(replace(outgoingScopedQuery, id, pagination)).map(toSparqlLinks)

    private def toSparqlLinks(sparqlResults: SparqlResults): LinkResults = {
      val (count, results) =
        sparqlResults.results.bindings
          .foldLeft((0L, List.empty[SparqlLink])) {
            case ((total, acc), bindings) =>
              val newTotal = bindings.get("total").flatMap(v => Try(v.value.toLong).toOption).getOrElse(total)
              val res      = (SparqlResourceLink(bindings) orElse SparqlExternalLink(bindings)).map(_ :: acc).getOrElse(acc)
              (newTotal, res)
          }
      UnscoredQueryResults(count, results.map(UnscoredQueryResult(_)))
    }

    /**
      * Generates the sparql index
      *
      * @param config the [[SparqlConfig]]
      */
    def index(implicit config: SparqlConfig): String = s"${config.indexPrefix}_$name"
  }

  object SparqlView {
    private val defaultViewId = UUID.fromString("d88b71d2-b8a4-4744-bf22-2d99ef5bd26b")

    /**
      * Default [[SparqlView]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef): SparqlView =
      // format: off
      SparqlView(Set.empty, Set.empty, None, includeMetadata = true, includeDeprecated = true, ref, nxv.defaultSparqlIndex.value, defaultViewId, 1L, deprecated = false)
      // format: on

  }

  /**
    * Aggregation of [[ElasticSearchView]].
    *
    * @param value      the set of elastic search views that this view connects to when performing searches
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
  ) extends AggregateView[P] {

    /**
      * Fetches each View from the ViewRefs and checks its deprecation status. It also checks if the permission for the project where the view is located
      * for the current client is ''views/query''.
      */
    def queryableIndices[F[_]](implicit projectCache: ProjectCache[F],
                               viewCache: ViewCache[F],
                               acls: AccessControlLists,
                               caller: Caller,
                               config: ElasticSearchConfig,
                               F: Monad[F]): F[Set[String]] =
      value match {
        case `Set[ViewRef[ProjectRef]]`(viewRefs) => queryableViews[F, ElasticSearchView](viewRefs).map(_.map(_.index))
        case _                                    => F.pure(Set.empty)
      }
  }

  /**
    * Aggregation of [[SparqlView]].
    *
    * @param value      the set of sparql views that this view connects to when performing searches
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class AggregateSparqlView[P](
      value: Set[ViewRef[P]],
      ref: ProjectRef,
      uuid: UUID,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean
  ) extends AggregateView[P] {

    /**
      * Fetches each View from the ViewRefs and checks its deprecation status. It also checks if the permission for the project where the view is located
      * for the current client is ''views/query''.
      */
    def queryableViews[F[_]](implicit projectCache: ProjectCache[F],
                             viewCache: ViewCache[F],
                             acls: AccessControlLists,
                             caller: Caller,
                             F: Monad[F]): F[Set[SparqlView]] =
      value match {
        case `Set[ViewRef[ProjectRef]]`(viewRefs) => queryableViews[F, SparqlView](viewRefs)
        case _                                    => F.pure(Set.empty)
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

  val `Set[ViewRef[ProjectRef]]`   = TypeCase[Set[ViewRef[ProjectRef]]]
  val `Set[ViewRef[ProjectLabel]]` = TypeCase[Set[ViewRef[ProjectLabel]]]
}
