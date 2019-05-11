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
import ch.epfl.bluebrain.nexus.kg.resolve.Materializer
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.Verify
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

class Storages[F[_]: Timer](repo: Repo[F])(implicit F: Effect[F], materializer: Materializer[F], config: AppConfig) {

  /**
    * Creates a new storage attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(source: Json)(implicit subject: Subject, verify: Verify[F], project: Project): RejOrResource[F] =
    materializer(source.addContext(storageCtxUri)).flatMap {
      case (id, Value(_, _, graph)) => create(Id(project.ref, id), graph)
    }

  /**
    * Creates a new storage.
    *
    * @param id     the id of the storage
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId,
             source: Json)(implicit subject: Subject, verify: Verify[F], project: Project): RejOrResource[F] =
    materializer(source.addContext(storageCtxUri), id.value).flatMap {
      case Value(_, _, graph) => create(id, graph)
    }

  /**
    * Updates an existing storage.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, source: Json)(implicit subject: Subject,
                                                 verify: Verify[F],
                                                 project: Project): RejOrResource[F] =
    for {
      _        <- repo.get(id, rev, Some(storageRef)).toRight(NotFound(id.ref, Some(rev)))
      matValue <- materializer(source.addContext(storageCtxUri), id.value)
      typedGraph = addStorageType(id.value, matValue.graph)
      types      = typedGraph.rootTypes.map(_.value)
      _       <- validateShacl(typedGraph)
      storage <- storageValidation(id, typedGraph, 1L, types)
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
    repo.get(id, rev, Some(storageRef)).toRight(NotFound(id.ref, Some(rev))).flatMap(_ => repo.deprecate(id, rev))

  /**
    * Fetches the latest revision of a storage.
    *
    * @param id the id of the resolver
    * @return Some(storage) in the F context when found and None in the F context when not found
    */
  def fetchStorage(id: ResId)(implicit project: Project): EitherT[F, Rejection, Storage] = {
    val repoOrNotFound = repo.get(id, Some(storageRef)).toRight(notFound(id.ref))
    repoOrNotFound.flatMap(fetch(_, dropKeys = false)).subflatMap(Storage(_, encrypt = false))
  }

  /**
    * Fetches the latest revision of a storage.
    *
    * @param id the id of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, Some(storageRef)).toRight(notFound(id.ref)).flatMap(fetch(_, dropKeys = true))

  /**
    * Fetches the provided revision of a storage
    *
    * @param id  the id of the storage
    * @param rev the revision of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, rev, Some(storageRef)).toRight(notFound(id.ref, Some(rev))).flatMap(fetch(_, dropKeys = true))

  /**
    * Fetches the provided tag of a storage.
    *
    * @param id  the id of the storage
    * @param tag the tag of the storage
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, tag, Some(storageRef)).toRight(notFound(id.ref, tagOpt = Some(tag))).flatMap(fetch(_, dropKeys = true))

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

  private def fetch(resource: Resource, dropKeys: Boolean)(implicit project: Project): RejOrResourceV[F] =
    materializer.withMeta(resource).map { resourceV =>
      val graph      = resourceV.value.graph
      val filter     = Set[IriNode](nxv.accessKey, nxv.secretKey, nxv.credentials)
      val finalGraph = if (dropKeys) graph.remove(p = filter.contains) else graph
      resourceV.map(_.copy(graph = RootedGraph(graph.rootNode, finalGraph)))
    }

  private def create(id: ResId, graph: RootedGraph)(implicit subject: Subject,
                                                    project: Project,
                                                    verify: Verify[F]): RejOrResource[F] = {
    val typedGraph = addStorageType(id.value, graph)
    val types      = typedGraph.rootTypes.map(_.value)

    for {
      _        <- validateShacl(typedGraph)
      storage  <- storageValidation(id, typedGraph, 1L, types)
      json     <- jsonForRepo(storage)
      resource <- repo.create(id, OrganizationRef(project.organizationUuid), storageRef, types, json)
    } yield resource
  }

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

  private def storageValidation(resId: ResId, graph: RootedGraph, rev: Long, types: Set[AbsoluteIri])(
      implicit verify: Verify[F]): EitherT[F, Rejection, Storage] = {
    val resource =
      ResourceF.simpleV(resId, Value(Json.obj(), Json.obj(), graph), rev = rev, types = types, schema = storageRef)

    EitherT.fromEither[F](Storage(resource, encrypt = true)).flatMap { storage =>
      EitherT(storage.isValid.apply).map(_ => storage).leftMap(msg => InvalidResourceFormat(resId.value.ref, msg))
    }
  }

  private def jsonForRepo(storage: Storage): EitherT[F, Rejection, Json] = {
    val graph                = storage.asGraph[CId].removeMetadata
    val jsonOrMarshallingErr = graph.as[Json](storageCtx).map(_.replaceContext(storageCtxUri))
    EitherT.fromEither[F](jsonOrMarshallingErr).leftSemiflatMap(fromMarshallingErr(storage.id, _)(F))
  }
}

object Storages {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Storages]] for the provided F type
    */
  final def apply[F[_]: Timer: Effect: Materializer](implicit config: AppConfig, repo: Repo[F]): Storages[F] =
    new Storages[F](repo)
}
