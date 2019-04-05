package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import cats.implicits._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{Caller, Identity}
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, IriOrBNode}
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams

class Resolvers[F[_]: Timer](repo: Repo[F])(implicit F: Effect[F], config: AppConfig, projectCache: ProjectCache[F]) {

  /**
    * Creates a new resolver attempting to extract the id from the source. If a primary node of the resulting graph
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
  def create(base: AbsoluteIri, source: Json)(implicit caller: Caller, project: Project): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph         <- materialize(transformedSource)
      assignedValue <- checkOrAssignId[F](base, Value(transformedSource, resolverCtx.contextValue, graph))
      (id, rootedGraph) = assignedValue
      resource <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Creates a new resolver.
    *
    * @param id     the id of the resolver
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, source: Json)(implicit caller: Caller): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, resolverCtx.contextValue, graph))
      resource    <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Updates an existing resolver.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, source: Json)(implicit caller: Caller): RejOrResource[F] =
    for {
      _ <- repo.get(id, rev, Some(resolverRef)).toRight(NotFound(id.ref, Some(rev)))
      transformedSource = transform(source)
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, resolverCtx.contextValue, graph))
      typedGraph = addResolverType(id.value, rootedGraph)
      types      = typedGraph.rootTypes.map(_.value)
      _        <- validateShacl(typedGraph)
      resolver <- resolverValidation(id, transformedSource, typedGraph, 1L, types)
      json     <- jsonForRepo(resolver)
      updated  <- repo.update(id, rev, types, json)
    } yield updated

  /**
    * Deprecates an existing resolver.
    *
    * @param id  the id of the resolver
    * @param rev the last known revision of the resolver
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long)(implicit subject: Subject): RejOrResource[F] =
    for {
      _          <- repo.get(id, rev, Some(resolverRef)).toRight(NotFound(id.ref, Some(rev)))
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

  /**
    * Fetches the latest revision of a resolver.
    *
    * @param id the id of the resolver
    * @return Some(resolver) in the F context when found and None in the F context when not found
    */
  def fetchResolver(id: ResId)(implicit project: Project): EitherT[F, Rejection, Resolver] =
    for {
      resource <- EitherT.fromOptionF(repo.get(id, Some(resolverRef)).value, notFound(id.ref))
      graph    <- materializeWithMeta(resource)
      view     <- EitherT.fromEither[F](Resolver(resource.map(Value(_, resolverCtx.contextValue, graph))))
    } yield view

  /**
    * Fetches the latest revision of a resolver.
    *
    * @param id the id of the resolver
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, Some(resolverRef)).value, notFound(id.ref)).flatMap(fetch)

  /**
    * Fetches the provided revision of a resolver
    *
    * @param id  the id of the resolver
    * @param rev the revision of the resolver
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, rev, Some(resolverRef)).value, notFound(id.ref, Some(rev))).flatMap(fetch)

  /**
    * Fetches the provided tag of a resolver.
    *
    * @param id  the id of the resolver
    * @param tag the tag of the resolver
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, tag, Some(resolverRef)).value, notFound(id.ref, tagOpt = Some(tag))).flatMap(fetch)

  /**
    * Lists resolvers on the given project
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elasticSearch: ElasticSearchClient[F]): F[JsonResults] =
    listResources(view, params.copy(schema = Some(resolverSchemaUri)), pagination)

  private def fetch(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    for {
      graph  <- materializeWithMeta(resource)
      output <- outputResource(resource.map(source => Value(source, resolverCtx.contextValue, graph)))
    } yield output

  private def create(id: ResId, source: Json, graph: RootedGraph)(implicit caller: Caller): RejOrResource[F] = {
    val typedGraph = addResolverType(id.value, graph)
    val types      = typedGraph.rootTypes.map(_.value)

    for {
      _        <- validateShacl(typedGraph)
      resolver <- resolverValidation(id, source, typedGraph, 1L, types)
      json     <- jsonForRepo(resolver)
      resource <- repo.create(id, resolverRef, types, json)
    } yield resource
  }

  private def materialize(source: Json, id: IriOrBNode = blank): EitherT[F, Rejection, RootedGraph] = {
    val valueOrMarshallingError = source.replaceContext(resolverCtx).asGraph(id)
    EitherT.fromEither[F](valueOrMarshallingError).leftSemiflatMap(fromMarshallingErr(_)(F))
  }

  private def materializeWithMeta(resource: Resource)(implicit project: Project): EitherT[F, Rejection, RootedGraph] =
    materialize(resource.value, resource.id.value).map(graph =>
      RootedGraph(graph.rootNode, graph.triples ++ resource.metadata()))

  private def addResolverType(id: AbsoluteIri, graph: RootedGraph): RootedGraph =
    RootedGraph(id, graph.triples + ((id.value, rdf.tpe, nxv.Resolver): Triple))

  private def validateShacl(data: RootedGraph): EitherT[F, Rejection, Unit] = {
    val model: CId[Model] = data.as[Model]()
    ShaclEngine(model, resolverSchemaModel, validateShapes = false, reportDetails = true) match {
      case Some(r) if r.isValid() => EitherT.rightT(())
      case Some(r)                => EitherT.leftT(InvalidResource(resolverRef, r))
      case _ =>
        EitherT(
          F.raiseError(InternalError(s"Unexpected error while attempting to validate schema '$resolverSchemaUri'")))
    }
  }

  private def resolverValidation(resId: ResId, source: Json, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit caller: Caller): EitherT[F, Rejection, Resolver] = {

    val noIdentities = "The caller doesn't have some of the provided identities on the resolver"

    def foundInCaller(identities: List[Identity]): Boolean =
      identities.forall(caller.identities.contains)

    val resource =
      ResourceF.simpleV(resId,
                        Value(source, resolverCtx.contextValue, graph),
                        rev = rev,
                        types = types,
                        schema = resolverRef)

    EitherT.fromEither[F](Resolver(resource)).flatMap {
      case r: CrossProjectResolver[_] if foundInCaller(r.identities) => r.referenced[F]
      case _: CrossProjectResolver[_]                                => EitherT.leftT[F, Resolver](InvalidIdentity(noIdentities))
      case r: InProjectResolver                                      => EitherT.rightT(r)
    }
  }

  private def jsonForRepo(resolver: Resolver): EitherT[F, Rejection, Json] = {
    val graph                = resolver.asGraph[CId].removeMetadata
    val jsonOrMarshallingErr = graph.as[Json](resolverCtx).map(_.replaceContext(resolverCtxUri))
    EitherT.fromEither[F](jsonOrMarshallingErr).leftSemiflatMap(fromMarshallingErr(_)(F))
  }

  private def transform(source: Json): Json = source.addContext(resolverCtxUri)

  private def outputResource(originalResource: ResourceV)(implicit project: Project): EitherT[F, Rejection, ResourceV] =
    Resolver(originalResource) match {
      case Right(resolver) =>
        resolver.labeled.flatMap { labeledResolver =>
          val graph = labeledResolver.asGraph[CId]
          val value =
            Value(originalResource.value.source,
                  resolverCtx.contextValue,
                  RootedGraph(graph.rootNode, graph.triples ++ originalResource.metadata()))
          EitherT.rightT(originalResource.copy(value = value))
        }
      case _ => EitherT.rightT(originalResource)
    }
}

object Resolvers {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Resolvers]] for the provided F type
    */
  final def apply[F[_]: Timer: Effect: ProjectCache](implicit config: AppConfig, repo: Repo[F]): Resolvers[F] =
    new Resolvers[F](repo)
}
