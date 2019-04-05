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
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller}
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateView, ElasticSearchView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.uuid
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, IriOrBNode}
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

class Views[F[_]: Timer](repo: Repo[F])(implicit F: Effect[F],
                                        config: AppConfig,
                                        projectCache: ProjectCache[F],
                                        viewCache: ViewCache[F],
                                        esClient: ElasticSearchClient[F]) {

  /**
    * Creates a new view attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param base       base used to generate new ids
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(base: AbsoluteIri,
             source: Json)(implicit acls: AccessControlLists, caller: Caller, project: Project): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph         <- materialize(transformedSource)
      assignedValue <- checkOrAssignId[F](base, Value(transformedSource, viewCtx.contextValue, graph))
      (id, rootedGraph) = assignedValue
      resource <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Creates a new view.
    *
    * @param id     the id of the view
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, source: Json)(implicit acls: AccessControlLists, caller: Caller): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, viewCtx.contextValue, graph))
      resource    <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Updates an existing view.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, source: Json)(implicit acls: AccessControlLists, caller: Caller): RejOrResource[F] =
    for {
      _ <- repo.get(id, rev, Some(viewRef)).toRight(NotFound(id.ref, Some(rev)))
      transformedSource = transform(source, extractUuidFrom(source))
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, viewCtx.contextValue, graph))
      typedGraph = addViewType(id.value, rootedGraph)
      types      = typedGraph.rootTypes.map(_.value)
      _       <- validateShacl(typedGraph)
      view    <- viewValidation(id, transformedSource, typedGraph, 1L, types)
      json    <- jsonForRepo(view)
      updated <- repo.update(id, rev, types, json)
    } yield updated

  /**
    * Deprecates an existing view.
    *
    * @param id  the id of the view
    * @param rev the last known revision of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long)(implicit subject: Subject): RejOrResource[F] =
    for {
      _          <- repo.get(id, rev, Some(viewRef)).toRight(NotFound(id.ref, Some(rev)))
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

  /**
    * Fetches the latest revision of a view.
    *
    * @param id the id of the resolver
    * @return Some(view) in the F context when found and None in the F context when not found
    */
  def fetchView(id: ResId)(implicit project: Project): EitherT[F, Rejection, View] =
    for {
      resource <- EitherT.fromOptionF(repo.get(id, Some(viewRef)).value, notFound(id.ref))
      graph    <- materializeWithMeta(resource)
      view     <- EitherT.fromEither[F](View(resource.map(Value(_, viewCtx.contextValue, graph))))
    } yield view

  /**
    * Fetches the latest revision of a view.
    *
    * @param id the id of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, Some(viewRef)).value, notFound(id.ref)).flatMap(fetch)

  /**
    * Fetches the provided revision of a view
    *
    * @param id  the id of the view
    * @param rev the revision of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, rev, Some(viewRef)).value, notFound(id.ref, Some(rev))).flatMap(fetch)

  /**
    * Fetches the provided tag of a view.
    *
    * @param id  the id of the view
    * @param tag the tag of the view
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, tag, Some(viewRef)).value, notFound(id.ref, tagOpt = Some(tag))).flatMap(fetch)

  /**
    * Lists views on the given project
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults]): F[JsonResults] =
    listResources[F](view, params.copy(schema = Some(viewSchemaUri)), pagination)

  private def fetch(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    for {
      graph  <- materializeWithMeta(resource)
      output <- outputResource(resource.map(source => Value(source, viewCtx.contextValue, graph)))
    } yield output

  private def create(id: ResId, source: Json, graph: RootedGraph)(implicit acls: AccessControlLists,
                                                                  caller: Caller): RejOrResource[F] = {
    val typedGraph = addViewType(id.value, graph)
    val types      = typedGraph.rootTypes.map(_.value)

    for {
      _        <- validateShacl(typedGraph)
      view     <- viewValidation(id, source, typedGraph, 1L, types)
      json     <- jsonForRepo(view)
      resource <- repo.create(id, viewRef, types, json)
    } yield resource
  }

  private def materialize(source: Json, id: IriOrBNode = blank): EitherT[F, Rejection, RootedGraph] = {
    val valueOrMarshallingError = source.replaceContext(viewCtx).asGraph(id)
    EitherT.fromEither[F](valueOrMarshallingError).leftSemiflatMap(fromMarshallingErr(_)(F))
  }

  private def materializeWithMeta(resource: Resource)(implicit project: Project): EitherT[F, Rejection, RootedGraph] =
    materialize(resource.value, resource.id.value).map(graph =>
      RootedGraph(graph.rootNode, graph.triples ++ resource.metadata()))

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

  private def viewValidation(resId: ResId, source: Json, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit acls: AccessControlLists,
      caller: Caller): EitherT[F, Rejection, View] = {
    val resource =
      ResourceF.simpleV(resId, Value(source, viewCtx.contextValue, graph), rev = rev, types = types, schema = viewRef)
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
    EitherT.fromEither[F](jsonOrMarshallingErr).leftSemiflatMap(fromMarshallingErr(_)(F))
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
            Value(originalResource.value.source,
                  viewCtx.contextValue,
                  RootedGraph(graph.rootNode, graph.triples ++ originalResource.metadata()))
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
  final def apply[F[_]: Timer: Effect: ProjectCache: ViewCache: ElasticSearchClient](implicit config: AppConfig,
                                                                                     repo: Repo[F]): Views[F] =
    new Views[F](repo)
}
