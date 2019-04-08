package ch.epfl.bluebrain.nexus.kg.resources

import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound.notFound
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.Resources.generateId
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileDescription
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.{Fetch, Save}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

class Files[F[_]: Effect: Timer](repo: Repo[F])(implicit config: AppConfig) {

  /**
    * Creates a file resource.
    *
    * @param projectRef reference for the project in which the resource is going to be created.\
    * @param base       base used to generate new ids
    * @param storage    the storage where the file is going to be saved
    * @param fileDesc   the file description metadata
    * @param source     the source of the file
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def create[In](projectRef: ProjectRef, base: AbsoluteIri, storage: Storage, fileDesc: FileDescription, source: In)(
      implicit subject: Subject,
      saveStorage: Save[F, In]): RejOrResource[F] =
    create(Id(projectRef, generateId(base)), storage, fileDesc, source)

  /**
    * Creates a file resource.
    *
    * @param id       the id of the resource
    * @param storage  the storage where the file is going to be saved
    * @param fileDesc the file description metadata
    * @param source   the source of the file
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def create[In](id: ResId, storage: Storage, fileDesc: FileDescription, source: In)(
      implicit subject: Subject,
      saveStorage: Save[F, In]): RejOrResource[F] =
    repo.createFile(id, storage, fileDesc, source)

  /**
    * Replaces a file resource.
    *
    * @param id       the id of the resource
    * @param storage  the storage where the file is going to be saved
    * @param rev      the last known revision of the resource
    * @param fileDesc the file description metadata
    * @param source   the source of the file
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def update[In](id: ResId, storage: Storage, rev: Long, fileDesc: FileDescription, source: In)(
      implicit subject: Subject,
      saveStorage: Save[F, In]): RejOrResource[F] =
    repo.updateFile(id, storage, rev, fileDesc, source)

  /**
    * Deprecates an existing file.
    *
    * @param id  the id of the file
    * @param rev the last known revision of the file
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long)(implicit subject: Subject): RejOrResource[F] =
    for {
      _          <- repo.get(id, rev, Some(fileRef)).toRight(NotFound(id.ref, Some(rev)))
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

  /**
    * Attempts to stream the file resource for the latest revision.
    *
    * @param id     the id of the resource
    * @return the optional streamed file in the F context
    */
  def fetch[Out](id: ResId)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetch(repo.get(id, Some(fileRef)).toRight(notFound(id.ref)))

  /**
    * Attempts to stream the file resource with specific revision.
    *
    * @param id     the id of the resource
    * @param rev    the revision of the resource
    * @return the optional streamed file in the F context
    */
  def fetch[Out](id: ResId, rev: Long)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetch(repo.get(id, rev, Some(fileRef)).toRight(notFound(id.ref, Some(rev))))

  /**
    * Attempts to stream the file resource with specific tag. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @return the optional streamed file in the F context
    */
  def fetch[Out](id: ResId, tag: String)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetch(repo.get(id, tag, Some(fileRef)).toRight(notFound(id.ref, tagOpt = Some(tag))))

  private def fetch[Out](rejOrResource: RejOrResource[F])(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    rejOrResource.subflatMap(resource => resource.file.toRight(NotFound(resource.id.ref))).flatMapF {
      case (storage, attr) => storage.fetch.apply(attr).map(out => Right((storage, attr, out)))
    }

  /**
    * Lists files on the given project
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elasticSearch: ElasticSearchClient[F]): F[JsonResults] =
    listResources(view, params.copy(schema = Some(fileSchemaUri)), pagination)
}

object Files {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Files]] for the provided F type
    */
  final def apply[F[_]: Timer: Effect](implicit config: AppConfig, repo: Repo[F]): Files[F] =
    new Files[F](repo)
}
