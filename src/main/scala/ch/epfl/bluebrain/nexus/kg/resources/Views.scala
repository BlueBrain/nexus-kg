package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.data.{EitherT, OptionT}
import cats.effect.Effect
import cats.implicits._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchClientError
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.{FromPagination, Pagination}
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller}
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Source.CrossProjectEventStream
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Materializer
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources.generateId
import ch.epfl.bluebrain.nexus.kg.resources.Views._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.Clients._
import ch.epfl.bluebrain.nexus.kg.routes.{Clients, SearchParams}
import ch.epfl.bluebrain.nexus.kg.uuid
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.apache.jena.rdf.model.Model

class Views[F[_]](repo: Repo[F])(
    implicit F: Effect[F],
    materializer: Materializer[F],
    config: AppConfig,
    projectCache: ProjectCache[F],
    viewCache: ViewCache[F],
    clients: Clients[F]
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
    for {
      materialized <- materializer(transformSave(source))
      (id, value) = materialized
      created <- create(Id(project.ref, id), value.graph)
    } yield created

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
    materializer(transformSave(source, sourceUuid), id.value).flatMap {
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
      matValue <- materializer(transformSave(source, extractUuidFrom(curr.value)), id.value)
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
    * Fetches the provided revision of the view.
    *
    * @param id  the id of the view
    * @param rev the revision of the view
    * @return Some(view) in the F context when found and None in the F context when not found
    */
  def fetchView(id: ResId, rev: Long)(implicit project: Project): EitherT[F, Rejection, View] =
    for {
      resource  <- repo.get(id, rev, Some(viewRef)).toRight(notFound(id.ref, rev = Some(rev), schema = Some(viewRef)))
      resourceV <- materializer.withMeta(resource)
      view      <- EitherT.fromEither[F](View(resourceV))
    } yield view

  /**
    * Fetches the latest revision of a view.
    *
    * @param id the id of the view
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
    repo.get(id, Some(viewRef)).map(_.value).map(transformFetchSource).toRight(notFound(id.ref, schema = Some(viewRef)))

  /**
    * Fetches the provided revision of the view source
    *
    * @param id     the id of the view
    * @param rev    the revision of the view
    * @return Right(source) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetchSource(id: ResId, rev: Long): RejOrSource[F] =
    repo
      .get(id, rev, Some(viewRef))
      .map(_.value)
      .map(transformFetchSource)
      .toRight(notFound(id.ref, rev = Some(rev), schema = Some(viewRef)))

  /**
    * Fetches the provided tag of the view source
    *
    * @param id     the id of the view
    * @param tag    the tag of the view
    * @return Right(source) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetchSource(id: ResId, tag: String): RejOrSource[F] =
    repo
      .get(id, tag, Some(viewRef))
      .map(_.value)
      .map(transformFetchSource)
      .toRight(notFound(id.ref, tag = Some(tag), schema = Some(viewRef)))

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
  def listIncoming(id: AbsoluteIri, view: Option[SparqlView], pagination: FromPagination): F[LinkResults] =
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
  ): F[LinkResults] =
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
    toEitherT(viewRef, ShaclEngine(model, viewSchemaModel, validateShapes = false, reportDetails = true))
  }

  private def viewValidation(resId: ResId, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit acls: AccessControlLists,
      caller: Caller
  ): EitherT[F, Rejection, View] = {
    val resource =
      ResourceF.simpleV(resId, Value(Json.obj(), Json.obj(), graph), rev = rev, types = types, schema = viewRef)
    EitherT.fromEither[F](View(resource)).flatMap {
      case es: ElasticSearchView => validateElasticSearchMappings(resId, es).map(_ => es)
      case agg: AggregateView =>
        agg.referenced[F].flatMap {
          case v: AggregateView =>
            val viewRefs = v.value.collect { case ViewRef(projectRef: ProjectRef, viewId) => projectRef -> viewId }
            val eitherFoundViews = viewRefs.toList.traverse {
              case (projectRef, viewId) =>
                OptionT(viewCache.get(projectRef).map(_.find(_.id == viewId))).toRight(notFound(viewId.ref))
            }
            eitherFoundViews.map(_ => v)
          case v => EitherT.rightT(v)
        }

      case view: CompositeView =>
        val identities = view.sourcesBy[CrossProjectEventStream].toSeq.flatMap(_.identities)
        if (identities.foundInCaller) view.referenced[F]
        else EitherT.leftT[F, View](InvalidIdentity())
      case view => view.referenced[F]
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

  private def transformSave(source: Json, uuidField: String = uuid())(implicit project: Project): Json = {
    val transformed = source.addContext(viewCtxUri) deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuidField))
    val withMapping = toText(transformed, nxv.mapping.prefix)
    val projectionsTransform = withMapping.hcursor
      .get[Vector[Json]](nxv.projections.prefix)
      .map { projections =>
        val pTransformed = projections.map { projection =>
          val flattened = toText(projection, nxv.mapping.prefix, nxv.context.prefix)
          val withId    = addIfMissing(flattened, "@id", generateId(project.base))
          addIfMissing(withId, nxv.uuid.prefix, UUID.randomUUID().toString)
        }
        withMapping deepMerge Json.obj(nxv.projections.prefix -> pTransformed.asJson)
      }
      .getOrElse(withMapping)

    projectionsTransform.hcursor
      .get[Vector[Json]](nxv.sources.prefix)
      .map { sources =>
        val sourceTransformed = sources.map { source =>
          val withId = addIfMissing(source, "@id", generateId(project.base))
          addIfMissing(withId, nxv.uuid.prefix, UUID.randomUUID().toString)
        }
        projectionsTransform deepMerge Json.obj(nxv.sources.prefix -> sourceTransformed.asJson)
      }
      .getOrElse(projectionsTransform)

  }

  private def addIfMissing[A: Encoder](json: Json, key: String, value: A): Json =
    if (json.hcursor.downField(key).succeeded) json else json deepMerge Json.obj(key -> value.asJson)

  private def toText(json: Json, fields: String*) =
    fields.foldLeft(json) { (acc, field) =>
      acc.hcursor.get[Json](field) match {
        case Right(value) if value.isObject => acc deepMerge Json.obj(field -> value.noSpaces.asJson)
        case _                              => acc
      }
    }

  private def extractUuidFrom(source: Json): String =
    source.hcursor.get[String](nxv.uuid.prefix).getOrElse(uuid())

  private def outputResource(
      originalResource: ResourceV
  )(implicit project: Project): EitherT[F, Rejection, ResourceV] = {

    def toGraph(v: View): EitherT[F, Rejection, ResourceF[Value]] = {
      val graph  = v.asGraph[CId]
      val rooted = RootedGraph(graph.rootNode, graph.triples ++ originalResource.metadata())
      EitherT.rightT(originalResource.copy(value = Value(originalResource.value.source, viewCtx.contextValue, rooted)))
    }
    View(originalResource).map(_.labeled[F].flatMap(toGraph)).getOrElse(EitherT.rightT(originalResource))
  }
}

object Views {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Views]] for the provided F type
    */
  final def apply[F[_]: Effect: ProjectCache: ViewCache: Clients: Materializer](
      implicit config: AppConfig,
      repo: Repo[F]
  ): Views[F] =
    new Views[F](repo)

  /**
    * Converts the inline json values
    */
  def transformFetch(json: Json): Json = {
    val withMapping = fromText(json, nxv.mapping.prefix)
    withMapping.hcursor
      .get[Vector[Json]](nxv.projections.prefix)
      .map { projections =>
        val transformed = projections.map { projection =>
          fromText(projection, nxv.mapping.prefix, nxv.context.prefix)
        }
        withMapping deepMerge Json.obj(nxv.projections.prefix -> transformed.asJson)
      }
      .getOrElse(withMapping)
  }

  private def transformFetchSource(json: Json): Json =
    transformFetch(json).removeNestedKeys(nxv.uuid.prefix)

  private def fromText(json: Json, fields: String*) =
    fields.foldLeft(json) { (acc, field) =>
      acc.hcursor.get[String](field).flatMap(parse) match {
        case Right(parsed) => acc deepMerge Json.obj(field -> parsed)
        case _             => acc
      }
    }
}
