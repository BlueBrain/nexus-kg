package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Instant
import java.util.regex.Pattern.quote
import java.util.{Properties, UUID}

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import cats.implicits._
import cats.{Functor, Monad}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.search.FromPagination
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults, SparqlWriteQuery}
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.CompositeViewConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import ch.epfl.bluebrain.nexus.kg.indexing.View.{query, read, AggregateView, CompositeView, ViewRef}
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection._
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Source.{CrossProjectEventStream, ProjectEventStream}
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.{Interval, Projection, Source}
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.metaKeys
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.kg.{identities, KgError}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import com.typesafe.scalalogging.Logger
import io.circe.{parser, Json}
import org.apache.jena.query.QueryFactory
import shapeless.Typeable.ValueTypeable
import shapeless.{TypeCase, Typeable}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

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
    * Converts the ProjectRefs into ProjectLabels when found on the cache
    */
  def labeled[F[_]: Monad](implicit projectCache: ProjectCache[F]): EitherT[F, Rejection, View] =
    this match {
      case v: AggregateView =>
        v.value.toList.traverse { case ViewRef(project, id) => project.toLabel[F].map(ViewRef(_, id)) }.map(v.make)
      case v: CompositeView =>
        val labeledSources: EitherT[F, Rejection, List[Source]] = v.sources.toList.traverse {
          case source: ProjectEventStream      => EitherT.rightT(source)
          case source: CrossProjectEventStream => source.project.toLabel[F].map(label => source.copy(project = label))
        }
        labeledSources.map(sources => v.copy(sources = sources.toSet))
      case other => EitherT.rightT(other)
    }

  /**
    * Converts the ProjectLabels into ProjectRefs when found on the cache
    * respecting the views/query and resources/read permissions
    */
  def referenced[F[_]: Monad](
      implicit projectCache: ProjectCache[F],
      acls: AccessControlLists,
      caller: Caller
  ): EitherT[F, Rejection, View] =
    this match {
      case v: AggregateView =>
        v.value.toList.traverse { case ViewRef(project, id) => project.toRef[F](query).map(ViewRef(_, id)) }.map(v.make)
      case v: CompositeView =>
        val labeledSources: EitherT[F, Rejection, List[Source]] = v.sources.toList.traverse {
          case source: ProjectEventStream => EitherT.rightT(source)
          case source: CrossProjectEventStream =>
            source.project.toRef[F](read).map(label => source.copy(project = label))
        }
        labeledSources.map(sources => v.copy(sources = sources.toSet))
      case other => EitherT.rightT(other)
    }
}

object View {

  val read: Permission     = Permission.unsafe("resources/read")
  val query: Permission    = Permission.unsafe("views/query")
  val write: Permission    = Permission.unsafe("views/write")
  private val idTemplating = "{resource_id}"

  /**
    * @param resourceSchemas set of schemas iris used in the view. Indexing will be triggered only for resources validated against any of those schemas (when empty, all resources are indexed)
    * @param resourceTypes set of types iris used in the view. Indexing will be triggered only for resources containing any of those types (when empty, all resources are indexed)
    * @param resourceTag tag used in the view. Indexing will be triggered only for resources containing the provided tag
    * @param includeDeprecated flag to include or exclude the deprecated resources on the indexed Document
    */
  final case class Filter(
      resourceSchemas: Set[AbsoluteIri] = Set.empty,
      resourceTypes: Set[AbsoluteIri] = Set.empty,
      resourceTag: Option[String] = None,
      includeDeprecated: Boolean = true
  )

  sealed trait FilteredView extends View {

    /**
      * @return filters the data to be indexed
      */
    def filter: Filter

    /**
      * Retrieves the latest state of the passed resource, according to the view filter
      *
      * @param resources the resources operations
      * @param event     the event
      * @return Some(resource) if found, None otherwise, wrapped in the effect type ''F[_]''
      */
    def toResource[F[_]: Functor](
        resources: Resources[F],
        event: Event
    )(implicit project: Project, metadataOpts: MetadataOptions): F[Option[ResourceV]] =
      filter.resourceTag
        .filter(_.trim.nonEmpty)
        .map(resources.fetch(event.id, _, metadataOpts, None))
        .getOrElse(resources.fetch(event.id, metadataOpts, None))
        .toOption
        .value

    /**
      * Evaluates if the provided resource has some of the types defined on the view filter.
      *
      * @param resource the resource
      */
    def allowedTypes(resource: ResourceV): Boolean =
      filter.resourceTypes.isEmpty || filter.resourceTypes.intersect(resource.types).nonEmpty

    /**
      * Evaluates if the provided resource has some of the schemas defined on the view filter.
      *
      * @param resource the resource
      */
    def allowedSchemas(resource: ResourceV): Boolean =
      filter.resourceSchemas.isEmpty || filter.resourceSchemas.contains(resource.schema.iri)

    /**
      * Evaluates if the provided resource has the tag defined on the view filter.
      *
      * @param resource the resource
      */
    def allowedTag(resource: ResourceV): Boolean =
      filter.resourceTag.forall(tag => resource.tags.get(tag).contains(resource.rev))
  }

  /**
    * Enumeration of indexed view types.
    */
  sealed trait IndexedView extends View {

    /**
      * The progress id for this view
      */
    def progressId(implicit config: AppConfig): String
  }

  /**
    * Enumeration of multiple view types.
    */
  sealed trait AggregateView extends View {

    /**
      * @return the set of views that this view connects to when performing searches
      */
    def value: Set[ViewRef]

    @SuppressWarnings(Array("RepeatedCaseBody"))
    def make(newValue: List[ViewRef]): AggregateView = this match {
      case agg: AggregateElasticSearchView => agg.copy(value = newValue.toSet)
      case agg: AggregateSparqlView        => agg.copy(value = newValue.toSet)
    }

    /**
      * Return the views with ''views/query'' permissions that are not deprecated from the provided ''viewRefs''
      *
      */
    def queryableViews[F[_], T <: View: Typeable](
        implicit projectCache: ProjectCache[F],
        F: Monad[F],
        viewCache: ViewCache[F],
        caller: Caller,
        acls: AccessControlLists
    ): F[Set[T]] =
      value.toList.foldM(Set.empty[T]) {
        case (acc, ViewRef(ref: ProjectRef, id)) =>
          (viewCache.getBy[T](ref, id) -> projectCache.getLabel(ref)).mapN {
            case (Some(view), Some(label)) if !view.deprecated && caller.hasPermission(acls, label, query) => acc + view
            case _                                                                                         => acc
          }
        case (acc, _) => F.pure(acc)
      }
  }

  /**
    * Enumeration of single view types.
    */
  sealed trait SingleView extends IndexedView with FilteredView {

    /**
      * The index value for this view
      */
    def index(implicit config: AppConfig): String

    /**
      * Attempts to create an index.
      */
    def createIndex[F[_]: Effect](implicit config: AppConfig, clients: Clients[F]): F[Unit]

    /**
      * Attempts to delete an index.
      */
    def deleteIndex[F[_]](implicit config: AppConfig, clients: Clients[F]): F[Boolean]

    /**
      * Attempts to delete the resource on the view index.
      *
      * @param resId the resource id to be deleted from the current index
      */
    def deleteResource[F[_]: Monad](resId: ResId)(implicit clients: Clients[F], config: AppConfig): F[Unit]

  }
  private def parse(string: String): NodeEncoder.EncoderResult[Json] =
    parser.parse(string).left.map(_ => IllegalConversion(""))

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param res a materialized resource
    * @return Right(view) if the resource is compatible with a View, Left(rejection) otherwise
    */
  final def apply(res: ResourceV)(implicit config: CompositeViewConfig): Either[Rejection, View] = {
    val c = res.value.graph.cursor()

    def filter(c: GraphCursor): Either[Rejection, Filter] =
      // format: off
      for {
        schemas <- c.downField(nxv.resourceSchemas).values.asListOf[AbsoluteIri].withDefault(List.empty).map(_.toSet).onError(res.id.ref, nxv.resourceSchemas.prefix)
        types <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].withDefault(List.empty).map(_.toSet).onError(res.id.ref, nxv.resourceTypes.prefix)
        tag <- c.downField(nxv.resourceTag).focus.asOption[String].flatMap(nonEmpty).onError(res.id.ref, nxv.resourceTag.prefix)
        includeDep <- c.downField(nxv.includeDeprecated).focus.as[Boolean].withDefault(true).onError(res.id.ref, nxv.includeDeprecated.prefix)
      } yield Filter(schemas, types, tag, includeDep)
    // format: on

    def elasticSearch(c: GraphCursor = c): Either[Rejection, ElasticSearchView] =
      // format: off
      for {
        uuid          <- c.downField(nxv.uuid).focus.as[UUID].onError(res.id.ref, nxv.uuid.prefix)
        mapping       <- c.downField(nxv.mapping).focus.as[String].flatMap(parse).onError(res.id.ref, nxv.mapping.prefix)
        f             <- filter(c)
        includeMeta   <- c.downField(nxv.includeMetadata).focus.as[Boolean].withDefault(false).onError(res.id.ref, nxv.includeMetadata.prefix)
        sourceAsText  <- c.downField(nxv.sourceAsText).focus.as[Boolean].withDefault(false).onError(res.id.ref, nxv.sourceAsText.prefix)
      } yield
        ElasticSearchView(mapping, f, includeMeta, sourceAsText, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
      // format: on

    def composite(): Either[Rejection, View] = {

      def validateSparqlQuery(id: AbsoluteIri, q: String): Either[Rejection, Unit] =
        Try(QueryFactory.create(q.replaceAll(quote(idTemplating), s"<${res.id.value.asString}>"))) match {
          case Success(_) if !q.contains(idTemplating) =>
            val err = s"The provided SparQL does not target an id. The templating '$idTemplating' should be present."
            Left(InvalidResourceFormat(id.ref, err))
          case Success(query) if query.isConstructType =>
            Right(())
          case Success(_) =>
            Left(InvalidResourceFormat(id.ref, "The provided SparQL query is not a CONSTRUCT query"))
          case Failure(err) =>
            Left(InvalidResourceFormat(id.ref, s"The provided SparQL query is invalid: Reason: '${err.getMessage}'"))
        }

      def validateRebuild(rebuildInterval: Option[FiniteDuration]): Either[Rejection, Unit] =
        if (rebuildInterval.forall(_ >= config.minIntervalRebuild))
          Right(())
        else
          Left(
            InvalidResourceFormat(res.id.ref, s"Rebuild interval cannot be smaller than '${config.minIntervalRebuild}'")
          )

      def elasticSearchProjection(c: GraphCursor): Either[Rejection, Projection] =
        for {
          id      <- c.focus.as[AbsoluteIri].onError(res.id.ref, "@id")
          query   <- c.downField(nxv.query).focus.as[String].onError(res.id.ref, nxv.query.prefix)
          _       <- validateSparqlQuery(id, query)
          view    <- elasticSearch(c)
          context <- c.downField(nxv.context).focus.as[String].flatMap(parse).onError(res.id.ref, nxv.context.prefix)
        } yield ElasticSearchProjection(query, view.copy(id = id), context)

      def checkNotAllowedSparql(id: AbsoluteIri): Either[Rejection, Unit] =
        if (id == nxv.defaultSparqlIndex.value)
          Left(InvalidResourceFormat(res.id.ref, s"'$id' cannot be '${nxv.defaultSparqlIndex}'."): Rejection)
        else
          Right(())

      def sparqlProjection(c: GraphCursor): Either[Rejection, Projection] =
        for {
          id    <- c.focus.as[AbsoluteIri].onError(res.id.ref, "@id")
          _     <- checkNotAllowedSparql(id)
          query <- c.downField(nxv.query).focus.as[String].onError(res.id.ref, nxv.query.prefix)
          _     <- validateSparqlQuery(id, query)
          view  <- sparql(c)
        } yield SparqlProjection(query, view.copy(id = id))

      def projections(iter: Iterable[GraphCursor]): Either[Rejection, Set[Projection]] =
        if (iter.size > config.maxProjections)
          Left(InvalidResourceFormat(res.id.ref, s"The number of projections cannot exceed ${config.maxProjections}"))
        else
          iter.toList
            .foldM(List.empty[Projection]) { (acc, innerCursor) =>
              innerCursor.downField(rdf.tpe).focus.as[AbsoluteIri].onError(res.id.ref, "@type").flatMap {
                case tpe if tpe == nxv.ElasticSearch.value => elasticSearchProjection(innerCursor).map(_ :: acc)
                case tpe if tpe == nxv.Sparql.value        => sparqlProjection(innerCursor).map(_ :: acc)
                case tpe =>
                  val err = s"projection @type with value '$tpe' is not supported."
                  Left(InvalidResourceFormat(res.id.ref, err): Rejection)
              }
            }
            .map(_.toSet): Either[Rejection, Set[Projection]]

      def currentProjectSource(c: GraphCursor): Either[Rejection, Source] =
        // format: off
        for {
          id          <- c.focus.as[AbsoluteIri].onError(res.id.ref, "@id")
          filter      <- filter(c)
          uuid        <- c.downField(nxv.uuid).focus.as[UUID].onError(id.ref, nxv.uuid.prefix)
          includeMeta <- c.downField(nxv.includeMetadata).focus.as[Boolean].withDefault(false).onError(id.ref, nxv.includeMetadata.prefix)
        } yield ProjectEventStream(id, uuid, filter, includeMeta)
      // format: on

      // format: off
      def crossProjectSource(c: GraphCursor): Either[Rejection, Source] =
        for {
          id                  <- c.focus.as[AbsoluteIri].onError(res.id.ref, "@id")
          filter              <- filter(c)
          uuid                <- c.downField(nxv.uuid).focus.as[UUID].onError(id.ref, nxv.uuid.prefix)
          includeMeta         <- c.downField(nxv.includeMetadata).focus.as[Boolean].withDefault(false).onError(id.ref, nxv.includeMetadata.prefix)
          projectIdentifier   <- c.downField(nxv.project).focus.as[ProjectIdentifier].onError(id.ref, "project")
          ids                 <- identities(res.id, c.downField(nxv.identities).downSet)
        } yield CrossProjectEventStream(id, uuid, filter, includeMeta, projectIdentifier, ids)
      // format: on

      def sources(iter: Iterable[GraphCursor]): Either[Rejection, Set[Source]] =
        if (iter.size > config.maxSources)
          Left(InvalidResourceFormat(res.id.ref, s"The number of sources cannot exceed ${config.maxSources}"))
        else
          iter.toList
            .foldM(List.empty[Source]) { (acc, innerCursor) =>
              innerCursor.downField(rdf.tpe).focus.as[AbsoluteIri].onError(res.id.ref, "@type").flatMap {
                case tpe if tpe == nxv.ProjectEventStream.value      => currentProjectSource(innerCursor).map(_ :: acc)
                case tpe if tpe == nxv.CrossProjectEventStream.value => crossProjectSource(innerCursor).map(_ :: acc)
                case tpe =>
                  val err = s"sources @type with value '$tpe' is not supported."
                  Left(InvalidResourceFormat(res.id.ref, err): Rejection)
              }
            }
            .map(_.toSet): Either[Rejection, Set[Source]]

      // format: off
      for {
        uuid          <- c.downField(nxv.uuid).focus.as[UUID].onError(res.id.ref, nxv.uuid.prefix)
        projections   <- projections(c.downField(nxv.projections).downSet)
        sources       <- sources(c.downField(nxv.sources).downSet)
        rebuildCursor  = c.downField(nxv.rebuildStrategy)
        rebuildTpe    <- rebuildCursor.downField(rdf.tpe).focus.asOption[AbsoluteIri].onError(res.id.ref, "@type")
        _             <- if(rebuildTpe.contains(nxv.Interval.value) || rebuildTpe.isEmpty)  Right(()) else Left(InvalidResourceFormat(res.id.ref, s"${nxv.rebuildStrategy.prefix} @type with value '$rebuildTpe' is not supported."))
        interval      <- rebuildCursor.downField(nxv.value).focus.asOption[FiniteDuration].onError(res.id.ref, "value")
        _             <- validateRebuild(interval)
      } yield CompositeView (sources, projections, interval.map(Interval), res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
      // format: on
    }

    def sparql(c: GraphCursor = c): Either[Rejection, SparqlView] =
      // format: off
      for {
        uuid          <- c.downField(nxv.uuid).focus.as[UUID].onError(res.id.ref, nxv.uuid.prefix)
        f             <- filter(c)
        includeMeta   <- c.downField(nxv.includeMetadata).focus.as[Boolean].withDefault(false).onError(res.id.ref, nxv.includeMetadata.prefix)
      } yield
        SparqlView(f, includeMeta, res.id.parent, res.id.value, uuid, res.rev, res.deprecated)
    // format: on

    def viewRefs(cursor: List[GraphCursor]): Either[Rejection, Set[ViewRef]] =
      cursor.foldM(Set.empty[ViewRef]) { (acc, blankC) =>
        for {
          project <- blankC.downField(nxv.project).focus.as[ProjectIdentifier].onError(res.id.ref, nxv.project.prefix)
          id      <- blankC.downField(nxv.viewId).focus.as[AbsoluteIri].onError(res.id.ref, nxv.viewId.prefix)
        } yield acc + ViewRef(project, id)
      }

    def aggregatedEsView(): Either[Rejection, View] =
      for {
        uuid     <- c.downField(nxv.uuid).focus.as[UUID].onError(res.id.ref, nxv.uuid.prefix)
        viewRefs <- viewRefs(c.downField(nxv.views).downSet.toList)
      } yield AggregateElasticSearchView(viewRefs, res.id.parent, uuid, res.id.value, res.rev, res.deprecated)

    def aggregatedSparqlView(): Either[Rejection, View] =
      for {
        uuid     <- c.downField(nxv.uuid).focus.as[UUID].onError(res.id.ref, nxv.uuid.prefix)
        viewRefs <- viewRefs(c.downField(nxv.views).downSet.toList)
      } yield AggregateSparqlView(viewRefs, res.id.parent, uuid, res.id.value, res.rev, res.deprecated)

    if (Set(nxv.View.value, nxv.ElasticSearchView.value).subsetOf(res.types)) elasticSearch()
    else if (Set(nxv.View.value, nxv.SparqlView.value).subsetOf(res.types)) sparql()
    else if (Set(nxv.View.value, nxv.CompositeView.value).subsetOf(res.types)) composite()
    else if (Set(nxv.View.value, nxv.AggregateElasticSearchView.value).subsetOf(res.types)) aggregatedEsView()
    else if (Set(nxv.View.value, nxv.AggregateSparqlView.value).subsetOf(res.types)) aggregatedSparqlView()
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
  }

  /**
    * ElasticSearch specific view.
    *
    * @param mapping         the ElasticSearch mapping for the index
    * @param filter          the filtering options for the view
    * @param includeMetadata flag to include or exclude metadata on the indexed Document
    * @param sourceAsText    flag to include or exclude the source Json as a blob
    * @param ref             a reference to the project that the view belongs to
    * @param id              the user facing view id
    * @param uuid            the underlying uuid generated for this view
    * @param rev             the view revision
    * @param deprecated      the deprecation state of the view
    */
  final case class ElasticSearchView(
      mapping: Json,
      filter: Filter,
      includeMetadata: Boolean,
      sourceAsText: Boolean,
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends SingleView {

    val ctx: Json = jsonContentOf("/elasticsearch/default-context.json")

    def index(implicit config: AppConfig): String = s"${config.elasticSearch.indexPrefix}_$name"

    def progressId(implicit config: AppConfig): String = s"elasticSearch-indexer-$index"

    def createIndex[F[_]](implicit F: Effect[F], config: AppConfig, clients: Clients[F]): F[Unit] =
      clients.elasticSearch
        .createIndex(index)
        .flatMap(_ => clients.elasticSearch.updateMapping(index, mapping))
        .flatMap {
          case true  => F.unit
          case false => F.raiseError(KgError.InternalError("View mapping validation could not be performed"))
        }

    def deleteIndex[F[_]](implicit config: AppConfig, clients: Clients[F]): F[Boolean] =
      clients.elasticSearch.deleteIndex(index)

    def deleteResource[F[_]](resId: ResId)(implicit F: Monad[F], clients: Clients[F], config: AppConfig): F[Unit] = {
      val client = clients.elasticSearch.withRetryPolicy(config.elasticSearch.indexing.retry)
      client.delete(index, resId.value.asString) >> F.unit
    }

    /**
      * Attempts to convert the passed resource to an ElasticSearch Document to be indexed.
      * The resulting document will have different Json shape depending on the view configuration.
      *
      * @param res the resource
      * @return Some(document) if the conversion was successful, None otherwise
      */
    def toDocument(
        res: ResourceV
    )(implicit metadataOpts: MetadataOptions, logger: Logger, config: AppConfig, project: Project): Option[Json] = {
      val rootNode = IriNode(res.id.value)

      val metaGraph    = if (includeMetadata) Graph(res.metadata(metadataOpts)) else Graph()
      val keysToRemove = if (includeMetadata) Seq.empty[String] else metaKeys

      def asJson(g: Graph): DecoderResult[Json] = RootedGraph(rootNode, g).as[Json](ctx)
      val transformed: DecoderResult[Json] =
        if (sourceAsText)
          asJson(metaGraph.add(rootNode, nxv.original_source, res.value.source.removeKeys(metaKeys: _*).noSpaces))
        else
          asJson(metaGraph).map(metaJson => res.value.source.removeKeys(keysToRemove: _*) deepMerge metaJson)

      transformed match {
        case Left(err) =>
          logger.error(
            s"Could not convert resource with id '${res.id}' and value '${res.value}' from Graph back to json. Reason: '${err.message}'"
          )
          None
        case Right(value) => Some(value.removeNestedKeys("@context"))
      }
    }

  }

  object ElasticSearchView {
    val allField = "_all_fields"
    implicit val elasticSearchTypeable: Typeable[ElasticSearchView] =
      ValueTypeable[ElasticSearchView, ElasticSearchView](classOf[ElasticSearchView], "ElasticSearchView")
    private val defaultViewId = UUID.fromString("684bd815-9273-46f4-ac1c-0383d4a98254")

    /**
      * Default [[ElasticSearchView]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef): ElasticSearchView = {
      val mapping = jsonContentOf("/elasticsearch/mapping.json")
      // format: off
      ElasticSearchView(mapping, Filter(), includeMetadata = true, sourceAsText = true, ref, nxv.defaultElasticSearchIndex.value, defaultViewId, 1L, deprecated = false)
      // format: on
    }
  }

  /**
    * Sparql specific view.
    *
    * @param filter          the filtering options for the view
    * @param includeMetadata flag to include or exclude metadata on the index
    * @param ref             a reference to the project that the view belongs to
    * @param id              the user facing view id
    * @param uuid            the underlying uuid generated for this view
    * @param rev             the view revision
    * @param deprecated      the deprecation state of the view
    */
  final case class SparqlView(
      filter: Filter,
      includeMetadata: Boolean,
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends SingleView {

    private def replace(query: String, id: AbsoluteIri, pagination: FromPagination): String =
      query
        .replaceAll(quote("{id}"), id.asString)
        .replaceAll(quote("{graph}"), (id + "graph").asString)
        .replaceAll(quote("{offset}"), pagination.from.toString)
        .replaceAll(quote("{size}"), pagination.size.toString)

    /**
      * Builds an Sparql INSERT query with all the triples of the passed resource
      *
      * @param res the resource
      * @return a DROP {...} INSERT DATA {triples} Sparql query
      */
    def buildInsertQuery(res: ResourceV): SparqlWriteQuery = {
      val graph = if (includeMetadata) res.value.graph else res.value.graph.removeMetadata
      SparqlWriteQuery.replace(res.id.toGraphUri, graph)
    }

    /**
      * Deletes the namedgraph where the triples for the resource are located inside the Sparql store.
      *
      * @param res the resource
      * @return a DROP {...} Sparql query
      */
    def buildDeleteQuery(res: ResourceV): SparqlWriteQuery =
      SparqlWriteQuery.drop(res.id.toGraphUri)

    /**
      * Runs incoming query using the provided SparqlView index against the provided [[BlazegraphClient]] endpoint
      *
      * @param id         the resource id. The query will select the incomings that match this id
      * @param pagination the pagination for the query
      * @tparam F the effect type
      */
    def incoming[F[_]: Functor](
        id: AbsoluteIri,
        pagination: FromPagination
    )(
        implicit client: BlazegraphClient[F],
        config: AppConfig
    ): F[LinkResults] =
      client.copy(namespace = index).queryRaw(replace(incomingQuery, id, pagination)).map(toSparqlLinks)

    /**
      * Runs outgoing query using the provided SparqlView index against the provided [[BlazegraphClient]] endpoint
      *
      * @param id                   the resource id. The query will select the incomings that match this id
      * @param pagination           the pagination for the query
      * @param includeExternalLinks flag to decide whether or not to include external links (not Nexus managed) in the query result
      * @tparam F the effect type
      */
    def outgoing[F[_]: Functor](id: AbsoluteIri, pagination: FromPagination, includeExternalLinks: Boolean)(
        implicit client: BlazegraphClient[F],
        config: AppConfig
    ): F[LinkResults] =
      if (includeExternalLinks)
        client.copy(namespace = index).queryRaw(replace(outgoingWithExternalQuery, id, pagination)).map(toSparqlLinks)
      else
        client.copy(namespace = index).queryRaw(replace(outgoingScopedQuery, id, pagination)).map(toSparqlLinks)

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

    def index(implicit config: AppConfig): String = s"${config.sparql.indexPrefix}_$name"

    def progressId(implicit config: AppConfig): String = s"sparql-indexer-$index"

    def createIndex[F[_]](implicit F: Effect[F], config: AppConfig, clients: Clients[F]): F[Unit] =
      clients.sparql.copy(namespace = index).createNamespace(properties) >> F.unit

    def deleteIndex[F[_]](implicit config: AppConfig, clients: Clients[F]): F[Boolean] =
      clients.sparql.copy(namespace = index).deleteNamespace

    def deleteResource[F[_]: Monad](resId: ResId)(implicit clients: Clients[F], config: AppConfig): F[Unit] = {
      val client = clients.sparql.copy(namespace = index).withRetryPolicy(config.elasticSearch.indexing.retry)
      client.drop(resId.toGraphUri)
    }
  }

  object SparqlView {
    implicit val sparqlTypeable: Typeable[SparqlView] =
      ValueTypeable[SparqlView, SparqlView](classOf[SparqlView], "SparqlView")
    private val defaultViewId = UUID.fromString("d88b71d2-b8a4-4744-bf22-2d99ef5bd26b")

    /**
      * Default [[SparqlView]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef): SparqlView =
      // format: off
      SparqlView(Filter(), includeMetadata = true, ref, nxv.defaultSparqlIndex.value, defaultViewId, 1L, deprecated = false)
      // format: on

    private val properties: Map[String, String] = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/blazegraph/index.properties"))
      props.asScala.toMap
    }
    private val incomingQuery: String             = contentOf("/blazegraph/incoming.txt")
    private val outgoingWithExternalQuery: String = contentOf("/blazegraph/outgoing_include_external.txt")
    private val outgoingScopedQuery: String       = contentOf("/blazegraph/outgoing_scoped.txt")
  }

  /**
    * Composite view. A source generates a temporary Sparql index which then is used to generate a set of indices
    *
    * @param sources          the source
    * @param projections     a set of indices created out of the temporary sparql index
    * @param rebuildStrategy the optional strategy to rebuild the projections
    * @param ref             a reference to the project that the view belongs to
    * @param id              the user facing view id
    * @param uuid            the underlying uuid generated for this view
    * @param rev             the view revision
    * @param deprecated      the deprecation state of the view
    */
  final case class CompositeView(
      sources: Set[Source],
      projections: Set[Projection],
      rebuildStrategy: Option[Interval],
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: UUID,
      rev: Long,
      deprecated: Boolean
  ) extends IndexedView {

    override def progressId(implicit config: AppConfig): String =
      defaultSparqlView.progressId

    def defaultSparqlView: SparqlView =
      SparqlView(Filter(), includeMetadata = true, ref, nxv.defaultSparqlIndex.value, uuid, rev, deprecated)

    def sparqlView(source: Source): SparqlView =
      SparqlView(source.filter, source.includeMetadata, ref, nxv.defaultSparqlIndex.value, uuid, rev, deprecated)

    def progressId(sourceId: AbsoluteIri): String = sourceId.asString

    def progressId(sourceId: AbsoluteIri, projectionId: AbsoluteIri): String = s"${sourceId}_$projectionId"

    def projectionsProgress(sourceIdOpt: Option[AbsoluteIri], projectionIdsOpt: Option[AbsoluteIri]): Set[String] =
      (sourceIdOpt, projectionIdsOpt) match {
        case (Some(sId), Some(pId)) => Set(progressId(sId, pId))
        case (Some(sId), _)         => projections.map(p => progressId(sId, p.view.id))
        case (_, Some(pId))         => sources.map(s => progressId(s.id, pId))
        case _                      => for (s <- sources; p <- projections) yield progressId(s.id, p.view.id)
      }

    def projectionsBy[T <: Projection: Typeable]: Set[T] = {
      val tpe = TypeCase[T]
      projections.collect { case tpe(projection) => projection }
    }

    def sourcesBy[T <: Source: Typeable]: Set[T] = {
      val tpe = TypeCase[T]
      sources.collect { case tpe(source) => source }
    }

    def source(sourceId: AbsoluteIri): Option[Source] =
      sources.find(_.id == sourceId)

    def projectSource(sourceId: AbsoluteIri): Option[ProjectRef] =
      source(sourceId).map {
        case CrossProjectEventStream(_, _, _, _, pRef: ProjectRef, _) => pRef
        case _                                                        => ref
      }

    def projectsSource: Set[ProjectRef] =
      sourcesBy[CrossProjectEventStream].collect {
        case CrossProjectEventStream(_, _, _, _, pRef: ProjectRef, _) => pRef
      } + ref

    def nextRestart(previous: Option[Instant]): Option[Instant] =
      (previous, rebuildStrategy).mapN { case (p, Interval(v)) => p.plusMillis(v.toMillis) }

  }

  object CompositeView {

    sealed trait Source extends Product with Serializable {
      def filter: Filter
      def includeMetadata: Boolean
      def id: AbsoluteIri
      def uuid: UUID
    }

    object Source {
      final case class ProjectEventStream(id: AbsoluteIri, uuid: UUID, filter: Filter, includeMetadata: Boolean)
          extends Source
      final case class CrossProjectEventStream(
          id: AbsoluteIri,
          uuid: UUID,
          filter: Filter,
          includeMetadata: Boolean,
          project: ProjectIdentifier,
          identities: List[Identity]
      ) extends Source
    }

    final case class Interval(value: FiniteDuration)

    sealed trait Projection extends Product with Serializable {
      def query: String
      def view: SingleView
      def indexResourceGraph[F[_]: Monad](res: ResourceV, graph: Graph)(
          implicit clients: Clients[F],
          config: AppConfig,
          metadataOpts: MetadataOptions,
          logger: Logger,
          project: Project
      ): F[Option[Unit]]

      /**
        * Runs a query replacing the {resource_id} with the resource id
        *
        * @param res the resource
        * @return a Sparql query results response
        */
      def runQuery[F[_]: Effect: Timer](res: ResourceV)(client: BlazegraphClient[F]): F[SparqlResults] =
        client.queryRaw(query.replaceAll(quote(idTemplating), s"<${res.id.value.asString}>"))
    }

    object Projection {
      final case class ElasticSearchProjection(query: String, view: ElasticSearchView, context: Json)
          extends Projection {

        /**
          * Attempts to convert the passed graph using the current context to an ElasticSearch Document to be indexed.
          * The resulting document will have different Json shape depending on the view configuration.
          *
          * @param res   the resource
          * @param graph the graph to be converted to a Document
          * @return Some(())) if the conversion was successful and the document was indexed, None otherwise
          */
        def indexResourceGraph[F[_]](
            res: ResourceV,
            graph: Graph
        )(
            implicit F: Monad[F],
            clients: Clients[F],
            config: AppConfig,
            metadataOpts: MetadataOptions,
            logger: Logger,
            project: Project
        ): F[Option[Unit]] = {
          val rootNode   = IriNode(res.id.value)
          val contextObj = Json.obj("@context" -> context)
          val finalCtx   = if (view.includeMetadata) view.ctx.appendContextOf(contextObj) else contextObj
          val metaGraph  = if (view.includeMetadata) Graph(graph.triples ++ res.metadata(metadataOpts)) else graph
          val client     = clients.elasticSearch.withRetryPolicy(config.elasticSearch.indexing.retry)
          RootedGraph(rootNode, metaGraph).as[Json](finalCtx) match {
            case Left(err) =>
              val msg =
                s"Could not convert resource with id '${res.id}' and graph '${graph.show}' from Graph back to json. Reason: '${err.message}'"
              logger.error(msg)
              F.pure(None)
            case Right(value) =>
              client.create(view.index, res.id.value.asString, value.removeNestedKeys("@context")) >> F.pure(
                Some(())
              )
          }
        }
      }

      final case class SparqlProjection(query: String, view: SparqlView) extends Projection {

        /**
          * Attempts index the passed graph triples into Sparql store.
          *
          * @param res   the resource
          * @param graph the graph to be indexed
          * @return Some(())) if the triples were indexed, None otherwise
          */
        def indexResourceGraph[F[_]](
            res: ResourceV,
            graph: Graph
        )(
            implicit F: Monad[F],
            clients: Clients[F],
            config: AppConfig,
            metadataOpts: MetadataOptions,
            logger: Logger,
            project: Project
        ): F[Option[Unit]] = {
          val client = clients.sparql.copy(namespace = view.index).withRetryPolicy(config.sparql.indexing.retry)
          client.replace(res.id.toGraphUri, graph) >> F.pure(Some(()))
        }
      }
    }
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
  final case class AggregateElasticSearchView(
      value: Set[ViewRef],
      ref: ProjectRef,
      uuid: UUID,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean
  ) extends AggregateView

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
  final case class AggregateSparqlView(
      value: Set[ViewRef],
      ref: ProjectRef,
      uuid: UUID,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean
  ) extends AggregateView

  /**
    * A view reference is a unique way to identify a view
    * @param project the project reference
    * @param id the view id
    */
  final case class ViewRef(project: ProjectIdentifier, id: AbsoluteIri)
}
