package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import cats.implicits._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchClientError
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.{FromPagination, Pagination}
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller}
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateView, ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Materializer
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.uuid
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

class Views[F[_]: Timer](repo: Repo[F])(
    implicit F: Effect[F],
    materializer: Materializer[F],
    config: AppConfig,
    projectCache: ProjectCache[F],
    viewCache: ViewCache[F],
    esClient: ElasticSearchClient[F]
) {

  /**
    * Creates a new view attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(source: Json)(implicit acls: AccessControlLists, caller: Caller, project: Project): RejOrResource[F] =
    materializer(transform(source)).flatMap {
      case (id, Value(_, _, graph)) => create(Id(project.ref, id), graph)
    }

  /**
    * Creates a new view.
    *
    * @param id          the id of the view
    * @param source      the source representation in json-ld format
    * @param extractUuid flag to decide whether to extract the uuid from the payload or to generate one
    * @return either a rejection or the newly created resource in the F context
    */
  def create(
      id: ResId,
      source: Json,
      extractUuid: Boolean = false
  )(implicit acls: AccessControlLists, caller: Caller, project: Project): RejOrResource[F] = {
    val sourceUuid = if (extractUuid) extractUuidFrom(source) else uuid()
    materializer(transform(source, sourceUuid), id.value).flatMap {
      case Value(_, _, graph) => create(id, graph)
    }
  }

  /**
    * Updates an existing view.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(
      id: ResId,
      rev: Long,
      source: Json
  )(implicit acls: AccessControlLists, caller: Caller, project: Project): RejOrResource[F] =
    for {
      curr     <- repo.get(id, Some(viewRef)).toRight(notFound(id.ref, schema = Some(viewRef)))
      matValue <- materializer(transform(source, extractUuidFrom(curr.value)), id.value)
      typedGraph = addViewType(id.value, matValue.graph)
      types      = typedGraph.rootTypes.map(_.value)
      _       <- validateShacl(typedGraph)
      view    <- viewValidation(id, typedGraph, 1L, types)
      json    <- jsonForRepo(view)
      updated <- repo.update(id, viewRef, rev, types, json)
      _       <- EitherT.right(viewCache.put(view))
    } yield updated

  /**
    * Deprecates an existing view.
    *
    * @param id  the id of the view
    * @param rev the last known revision of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long)(implicit subject: Subject): RejOrResource[F] =
    repo.deprecate(id, viewRef, rev)

  /**
    * Fetches the latest revision of a view.
    *
    * @param id the id of the resolver
    * @return Some(view) in the F context when found and None in the F context when not found
    */
  def fetchView(id: ResId)(implicit project: Project): EitherT[F, Rejection, View] =
    for {
      resource  <- repo.get(id, Some(viewRef)).toRight(notFound(id.ref, schema = Some(viewRef)))
      resourceV <- materializer.withMeta(resource)
      view      <- EitherT.fromEither[F](View(resourceV))
    } yield view

  /**
    * Fetches the latest revision of the view source
    *
    * @param id the id of the view
    * @return Right(source) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetchSource(id: ResId): RejOrSource[F] =
    repo.get(id, Some(viewRef)).map(_.value).toRight(notFound(id.ref, schema = Some(viewRef)))

  /**
    * Fetches the provided revision of the view source
    *
    * @param id     the id of the view
    * @param rev    the revision of the view
    * @return Right(source) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetchSource(id: ResId, rev: Long): RejOrSource[F] =
    repo.get(id, rev, Some(viewRef)).map(_.value).toRight(notFound(id.ref, rev = Some(rev), schema = Some(viewRef)))

  /**
    * Fetches the provided tag of the view source
    *
    * @param id     the id of the view
    * @param tag    the tag of the view
    * @return Right(source) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetchSource(id: ResId, tag: String): RejOrSource[F] =
    repo.get(id, tag, Some(viewRef)).map(_.value).toRight(notFound(id.ref, tag = Some(tag), schema = Some(viewRef)))

  /**
    * Fetches the latest revision of a view.
    *
    * @param id the id of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, Some(viewRef)).toRight(notFound(id.ref, schema = Some(viewRef))).flatMap(fetch)

  /**
    * Fetches the provided revision of a view
    *
    * @param id  the id of the view
    * @param rev the revision of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, rev, Some(viewRef)).toRight(notFound(id.ref, Some(rev), schema = Some(viewRef))).flatMap(fetch)

  /**
    * Fetches the provided tag of a view.
    *
    * @param id  the id of the view
    * @param tag the tag of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, tag, Some(viewRef)).toRight(notFound(id.ref, tag = Some(tag), schema = Some(viewRef))).flatMap(fetch)

  /**
    * Lists views on the given project
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults]
  ): F[JsonResults] =
    listResources[F](view, params.copy(schema = Some(viewSchemaUri)), pagination)

  /**
    * Lists incoming resources for the provided ''id''
    *
    * @param id         the resource id for which to retrieve the incoming links
    * @param view       optionally available default sparql view
    * @param pagination pagination options
    * @return search results in the F context
    */
  def listIncoming(id: AbsoluteIri, view: Option[SparqlView], pagination: FromPagination)(
      implicit sparql: BlazegraphClient[F]
  ): F[LinkResults] =
    incoming(id, view, pagination)

  /**
    * Lists outgoing resources for the provided ''id''
    *
    * @param id                   the resource id for which to retrieve the outgoing links
    * @param view                 optionally available default sparql view
    * @param pagination           pagination options
    * @param includeExternalLinks flag to decide whether or not to include external links (not Nexus managed) in the query result
    * @return search results in the F context
    */
  def listOutgoing(
      id: AbsoluteIri,
      view: Option[SparqlView],
      pagination: FromPagination,
      includeExternalLinks: Boolean
  )(implicit sparql: BlazegraphClient[F]): F[LinkResults] =
    outgoing(id, view, pagination, includeExternalLinks)

  private def fetch(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    materializer.withMeta(resource).flatMap(outputResource)

  private def create(
      id: ResId,
      graph: RootedGraph
  )(implicit acls: AccessControlLists, project: Project, caller: Caller): RejOrResource[F] = {
    val typedGraph = addViewType(id.value, graph)
    val types      = typedGraph.rootTypes.map(_.value)

    for {
      _       <- validateShacl(typedGraph)
      view    <- viewValidation(id, typedGraph, 1L, types)
      json    <- jsonForRepo(view)
      created <- repo.create(id, OrganizationRef(project.organizationUuid), viewRef, types, json)
      _       <- EitherT.right(viewCache.put(view))
    } yield created
  }

  private def addViewType(id: AbsoluteIri, graph: RootedGraph): RootedGraph =
    RootedGraph(id, graph.triples + ((id.value, rdf.tpe, nxv.View): Triple))

  private def validateShacl(data: RootedGraph): EitherT[F, Rejection, Unit] = {
    val model: CId[Model] = data.as[Model]()
    ShaclEngine(model, viewSchemaModel, validateShapes = false, reportDetails = true) match {
      case Some(r) if r.isValid() => EitherT.rightT(())
      case Some(r)                => EitherT.leftT(InvalidResource(viewRef, r))
      case _ =>
        EitherT(F.raiseError(InternalError(s"Unexpected error while attempting to validate schema '$viewSchemaUri'")))
    }
  }

  private def viewValidation(resId: ResId, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit acls: AccessControlLists,
      caller: Caller
  ): EitherT[F, Rejection, View] = {
    val resource =
      ResourceF.simpleV(resId, Value(Json.obj(), Json.obj(), graph), rev = rev, types = types, schema = viewRef)
    EitherT.fromEither[F](View(resource)).flatMap {
      case es: ElasticSearchView => validateElasticSearchMappings(resId, es).map(_ => es)
      case agg: AggregateView[_] => agg.referenced[F](caller, acls)
      case view                  => EitherT.rightT(view)
    }
  }

  private def validateElasticSearchMappings(resId: ResId, es: ElasticSearchView): RejOrUnit[F] =
    EitherT(es.createIndex[F].map[Either[Rejection, Unit]](_ => Right(())).recoverWith {
      case ElasticSearchClientError(_, body) => F.pure(Left(InvalidResourceFormat(resId.ref, body)))
    })

  private def jsonForRepo(view: View): EitherT[F, Rejection, Json] = {
    val graph                = view.asGraph[CId].removeMetadata
    val jsonOrMarshallingErr = graph.as[Json](viewCtx).map(_.replaceContext(viewCtxUri))
    EitherT.fromEither[F](jsonOrMarshallingErr).leftSemiflatMap(fromMarshallingErr(view.id, _)(F))
  }

  private def transform(source: Json, uuidField: String = uuid()): Json = {
    val transformed = source.addContext(viewCtxUri) deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuidField))
    transformed.hcursor.get[Json]("mapping") match {
      case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
      case _                      => transformed
    }
  }

  private def extractUuidFrom(source: Json): String =
    source.hcursor.get[String](nxv.uuid.prefix).getOrElse(uuid())

  private def outputResource(originalResource: ResourceV)(implicit project: Project): EitherT[F, Rejection, ResourceV] =
    View(originalResource) match {
      case Right(view) =>
        view.labeled.flatMap { labeledView =>
          val graph = labeledView.asGraph[CId]
          val value =
            Value(
              originalResource.value.source,
              viewCtx.contextValue,
              RootedGraph(graph.rootNode, graph.triples ++ originalResource.metadata())
            )
          EitherT.rightT(originalResource.copy(value = value))
        }
      case _ => EitherT.rightT(originalResource)
    }
}

object Views {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Views]] for the provided F type
    */
  final def apply[F[_]: Timer: Effect: ProjectCache: ViewCache: ElasticSearchClient: Materializer](
      implicit config: AppConfig,
      repo: Repo[F]
  ): Views[F] =
    new Views[F](repo)
}
