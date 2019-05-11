package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.commons.shacl.{ShaclEngine, ValidationReport}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resolve.Materializer
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound.notFound
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.blank
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import io.circe.Json
import org.apache.jena.rdf.model.Model

/**
  * Resource operations.
  */
class Resources[F[_]: Timer](implicit F: Effect[F],
                             val repo: Repo[F],
                             materializer: Materializer[F],
                             config: AppConfig) {

  /**
    * Creates a new resource attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param schema     a schema reference that constrains the resource
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(schema: Ref, source: Json)(implicit subject: Subject, project: Project): RejOrResource[F] =
    materializer(source).flatMap {
      case (id, Value(_, _, graph)) => create(Id(project.ref, id), schema, source, graph.removeMetadata)
    }

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, schema: Ref, source: Json)(implicit subject: Subject, project: Project): RejOrResource[F] =
    materializer(source, id.value).flatMap {
      case Value(_, _, graph) => create(id, schema, source, graph.removeMetadata)
    }

  private def create(id: ResId, schema: Ref, source: Json, graph: RootedGraph)(implicit subject: Subject,
                                                                               project: Project): RejOrResource[F] =
    validate(schema, graph).flatMap { _ =>
      repo.create(id, OrganizationRef(project.organizationUuid), schema, graph.types(id.value).map(_.value), source)
    }

  /**
    * Updates an existing resource.
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param schema the schema reference that constrains the resource
    * @param source the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, schema: Ref, source: Json)(implicit subject: Subject,
                                                              project: Project): RejOrResource[F] =
    for {
      _        <- repo.get(id, rev, Some(schema)).toRight(NotFound(id.ref, Some(rev)))
      matValue <- materializer(source, id.value)
      graph = matValue.graph.removeMetadata
      _       <- validate(schema, graph)
      updated <- repo.update(id, rev, graph.types(id.value).map(_.value), source)
    } yield updated

  /**
    * Fetches the latest revision of a resource
    *
    * @param id     the id of the resource
    * @param schema the schema reference that constrains the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, schema: Ref)(implicit project: Project): RejOrResourceV[F] =
    fetch(id, selfAsIri = false).check(schema)

  /**
    * Fetches the provided revision of a resource
    *
    * @param id     the id of the resource
    * @param rev    the revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, rev: Long, schema: Ref)(implicit project: Project): RejOrResourceV[F] =
    fetch(id, rev, selfAsIri = false).check(schema)

  /**
    * Fetches the provided tag of a resource
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String, schema: Ref)(implicit project: Project): RejOrResourceV[F] =
    fetch(id, tag, selfAsIri = false).check(schema)

  /**
    * Fetches the latest revision of a resource
    *
    * @param id     the id of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, selfAsIri: Boolean)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, None).toRight(notFound(id.ref)).flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Fetches the provided revision of a resource
    *
    * @param id     the id of the resource
    * @param rev    the rev of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, rev: Long, selfAsIri: Boolean)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, rev, None).toRight(notFound(id.ref, Some(rev))).flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Fetches the provided tag of a resource
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, tag: String, selfAsIri: Boolean)(implicit project: Project): RejOrResourceV[F] =
    repo.get(id, tag, None).toRight(notFound(id.ref, tagOpt = Some(tag))).flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Deprecates an existing resource
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long, schema: Ref)(implicit subject: Subject): RejOrResource[F] =
    repo.get(id, rev, Some(schema)).toRight(NotFound(id.ref, Some(rev))).flatMap(_ => repo.deprecate(id, rev))

  /**
    * Lists resources for the given project and schema
    *
    * @param view       optionally available default elasticSearch view
    * @param params     filter parameters of the resources
    * @param pagination pagination options
    * @return search results in the F context
    */
  def list(view: Option[ElasticSearchView], params: SearchParams, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elasticSearch: ElasticSearchClient[F]): F[JsonResults] =
    listResources(view, params, pagination)

  private def validate(schema: Ref, data: Graph)(implicit project: Project): EitherT[F, Rejection, Unit] = {
    def toEitherT(optReport: Option[ValidationReport]): EitherT[F, Rejection, Unit] =
      optReport match {
        case Some(r) if r.isValid() => EitherT.rightT(())
        case Some(r)                => EitherT.leftT(InvalidResource(schema, r))
        case _ =>
          val err = InternalError(s"Unexpected error while attempting to validate schema '${schema.iri.asString}'")
          EitherT(F.raiseError(err))
      }

    def partition(set: Set[ResourceV]): (Set[ResourceV], Set[ResourceV]) =
      set.partition(_.isSchema)

    def schemaContext(): EitherT[F, Rejection, SchemaContext] =
      for {
        resourceSchema    <- materializer(schema)
        importedResources <- materializer.imports(resourceSchema.id, resourceSchema.value.graph)
        (schemaImports, dataImports) = partition(importedResources)
      } yield SchemaContext(resourceSchema, dataImports, schemaImports)

    schema.iri match {
      case `unconstrainedSchemaUri` => EitherT.rightT(())
      case _ =>
        schemaContext().flatMap { resolved =>
          val resolvedSchemaSets =
            resolved.schemaImports.foldLeft(resolved.schema.value.graph.triples)(_ ++ _.value.graph.triples)
          val resolvedSchema   = RootedGraph(blank, resolvedSchemaSets).as[Model]()
          val resolvedDataSets = resolved.dataImports.foldLeft(data.triples)(_ ++ _.value.graph.triples)
          val resolvedData     = RootedGraph(blank, resolvedDataSets).as[Model]()
          toEitherT(ShaclEngine(resolvedData, resolvedSchema, validateShapes = false, reportDetails = true))
        }
    }
  }

  private final implicit class ResourceSchemaSyntax(private val resourceV: RejOrResourceV[F]) {
    def check(schema: Ref): RejOrResourceV[F] =
      resourceV.subflatMap {
        case resource if resource.schema == schema => Right(resource)
        case _                                     => Left(NotFound(schema))
      }
  }
}

object Resources {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Resources]] for the provided F type
    */
  final def apply[F[_]: Timer: Repo: Effect: Materializer](implicit config: AppConfig): Resources[F] =
    new Resources[F]()

  private[resources] final case class SchemaContext(schema: ResourceV,
                                                    dataImports: Set[ResourceV],
                                                    schemaImports: Set[ResourceV])

  def getOrAssignId(json: Json)(implicit project: Project): AbsoluteIri =
    json.id.getOrElse(generateId(project.base))

  private[resources] def generateId(base: AbsoluteIri): AbsoluteIri = url"${base.asString}${uuid()}"

}
