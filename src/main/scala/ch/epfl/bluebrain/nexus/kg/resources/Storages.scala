package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.Verify
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
<<<<<<< HEAD
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, IriNode}
=======
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, IriOrBNode}
>>>>>>> Added routes and materialization fixes
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

class Storages[F[_]: Timer](repo: Repo[F])(implicit F: Effect[F], config: AppConfig) {

  /**
    * Creates a new storage attempting to extract the id from the source. If a primary node of the resulting graph
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
             source: Json)(implicit subject: Subject, verify: Verify[F], project: Project): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph         <- materialize(transformedSource)
      assignedValue <- checkOrAssignId[F](base, Value(transformedSource, storageCtx.contextValue, graph))
      (id, rootedGraph) = assignedValue
      resource <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Creates a new storage.
    *
    * @param id     the id of the storage
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, source: Json)(implicit subject: Subject, verify: Verify[F]): RejOrResource[F] = {
    val transformedSource = transform(source)
    for {
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, storageCtx.contextValue, graph))
      resource    <- create(id, transformedSource, rootedGraph)
    } yield resource
  }

  /**
    * Updates an existing storage.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, source: Json)(implicit subject: Subject, verify: Verify[F]): RejOrResource[F] =
    for {
      _ <- repo.get(id, rev, Some(storageRef)).toRight(NotFound(id.ref))
      transformedSource = transform(source)
      graph       <- materialize(transformedSource, id.value)
      rootedGraph <- checkId[F](id, Value(transformedSource, storageCtx.contextValue, graph))
      typedGraph = addStorageType(id.value, rootedGraph)
      types      = typedGraph.rootTypes.map(_.value)
      _       <- validateShacl(typedGraph)
      storage <- storageValidation(id, transformedSource, typedGraph, 1L, types)
      json    <- jsonForRepo(storage)
      updated <- repo.update(id, rev, types, json)
    } yield updated

  /**
    * Deprecates an existing storage.
    *
    * @param id  the id of the storage
    * @param rev the last known revision of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long)(implicit subject: Subject): RejOrResource[F] =
    for {
      _          <- repo.get(id, rev, Some(storageRef)).toRight(NotFound(id.ref))
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

  /**
    * Fetches the latest revision of a storage.
    *
    * @param id the id of the resolver
    * @return Some(storage) in the F context when found and None in the F context when not found
    */
  def fetchStorage(id: ResId)(implicit project: Project): EitherT[F, Rejection, Storage] =
    fetch(id).subflatMap(Storage(_, encrypt = false))

  /**
    * Fetches the latest revision of a storage.
    *
    * @param id the id of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, Some(storageRef)).value, notFound(id.ref)).flatMap(fetch)

  /**
    * Fetches the provided revision of a storage
    *
    * @param id  the id of the storage
    * @param rev the revision of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, rev, Some(storageRef)).value, notFound(id.ref, Some(rev))).flatMap(fetch)

  /**
    * Fetches the provided tag of a storage.
    *
    * @param id  the id of the storage
    * @param tag the tag of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String)(implicit project: Project): RejOrResourceV[F] =
    EitherT.fromOptionF(repo.get(id, tag, Some(storageRef)).value, notFound(id.ref, tagOpt = Some(tag))).flatMap(fetch)

  /**
    * Lists storages on the given project
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elasticSearch: ElasticSearchClient[F]): F[JsonResults] =
    listResources(view, params.copy(schema = Some(storageSchemaUri)), pagination)

  private def fetch(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    materializeWithMeta(resource).map { graph =>
      val filter = Set[IriNode](nxv.accessKey, nxv.secretKey)
      resource.map(source =>
        Value(source, storageCtx.contextValue, RootedGraph(graph.rootNode, graph.remove(p = filter.contains))))
    }

  private def create(id: ResId, source: Json, graph: RootedGraph)(implicit subject: Subject,
                                                                  verify: Verify[F]): RejOrResource[F] = {
    val typedGraph = addStorageType(id.value, graph)
    val types      = typedGraph.rootTypes.map(_.value)

    for {
      _        <- validateShacl(typedGraph)
      storage  <- storageValidation(id, source, typedGraph, 1L, types)
      json     <- jsonForRepo(storage)
      resource <- repo.create(id, storageRef, types, json)
    } yield resource
  }

  private def materialize(source: Json, id: IriOrBNode = blank): EitherT[F, Rejection, RootedGraph] = {
    val valueOrMarshallingError = source.replaceContext(storageCtx).asGraph(id)
    EitherT.fromEither[F](valueOrMarshallingError).leftSemiflatMap(fromMarshallingErr(_)(F))
  }

  private def materializeWithMeta(resource: Resource)(implicit project: Project): EitherT[F, Rejection, RootedGraph] =
    materialize(resource.value, resource.id.value).map(graph =>
      RootedGraph(graph.rootNode, graph.triples ++ resource.metadata()))

  private def addStorageType(id: AbsoluteIri, graph: RootedGraph): RootedGraph =
    RootedGraph(id, graph.triples + ((id.value, rdf.tpe, nxv.Storage): Triple))

  private def validateShacl(data: RootedGraph): EitherT[F, Rejection, Unit] = {
    val model: CId[Model] = data.as[Model]()
    ShaclEngine(model, storageSchemaModel, validateShapes = false, reportDetails = true) match {
      case Some(r) if r.isValid() => EitherT.rightT(())
      case Some(r)                => EitherT.leftT(InvalidResource(storageRef, r))
      case _ =>
        EitherT(
          F.raiseError(InternalError(s"Unexpected error while attempting to validate schema '$storageSchemaUri'")))
    }
  }

  private def storageValidation(resId: ResId, source: Json, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit verify: Verify[F]): EitherT[F, Rejection, Storage] = {
    val resource =
      ResourceF.simpleV(resId,
                        Value(source, storageCtx.contextValue, graph),
                        rev = rev,
                        types = types,
                        schema = storageRef)

    EitherT.fromEither[F](Storage(resource, encrypt = true)).flatMap { storage =>
      EitherT(storage.isValid.apply).map(_ => storage).leftMap(msg => InvalidResourceFormat(resId.value.ref, msg))
    }
  }

  private def jsonForRepo(storage: Storage): EitherT[F, Rejection, Json] = {
    val graph                = storage.asGraph[CId].removeMetadata
    val jsonOrMarshallingErr = graph.as[Json](storageCtx).map(_.replaceContext(storageCtxUri))
    EitherT.fromEither[F](jsonOrMarshallingErr).leftSemiflatMap(fromMarshallingErr(_)(F))
  }

  private def transform(source: Json): Json = source.addContext(storageCtxUri)
}

object Storages {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Storages]] for the provided F type
    */
  final def apply[F[_]: Timer: Effect](implicit config: AppConfig, repo: Repo[F]): Storages[F] =
    new Storages[F](repo)
}
