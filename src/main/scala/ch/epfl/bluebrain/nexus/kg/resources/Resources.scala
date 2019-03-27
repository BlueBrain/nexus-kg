package ch.epfl.bluebrain.nexus.kg.resources

import cats.Applicative
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
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound.notFound
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.Resources.{SchemaContext, _}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, BNode, IriNode, IriOrBNode}
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
    * @param base       base used to generate new ids.
    * @param schema     a schema reference that constrains the resource
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(base: AbsoluteIri, schema: Ref, source: Json)(implicit subject: Subject,
                                                           project: Project): RejOrResource[F] =
    for {
      matValue      <- materializer(source)
      assignedValue <- checkOrAssignId[F](base, matValue.copy(graph = matValue.graph.removeMetadata))
      (id, rootedGraph) = assignedValue
      created <- create(id, schema, source, rootedGraph)
    } yield created

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, schema: Ref, source: Json)(implicit subject: Subject, project: Project): RejOrResource[F] =
    for {
      matValue    <- materializer(source)
      rootedGraph <- checkId[F](id, matValue.copy(graph = matValue.graph.removeMetadata))
      created     <- create(id, schema, source, rootedGraph)
    } yield created

  private def create(id: ResId, schema: Ref, source: Json, graph: RootedGraph)(implicit subject: Subject,
                                                                               project: Project): RejOrResource[F] =
    for {
      _       <- validate(schema, graph)
      created <- repo.create(id, schema, graph.types(id.value).map(_.value), source)
    } yield created

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
      _           <- repo.get(id, rev, Some(schema)).toRight(NotFound(id.ref, Some(rev)))
      matValue    <- materializer(source)
      rootedGraph <- checkId[F](id, matValue.copy(graph = matValue.graph.removeMetadata))
      _           <- validate(schema, rootedGraph)
      updated     <- repo.update(id, rev, rootedGraph.types(id.value).map(_.value), source)
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
    EitherT.fromOptionF(repo.get(id, None).value, notFound(id.ref)).flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Fetches the provided revision of a resource
    *
    * @param id     the id of the resource
    * @param rev    the rev of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, rev: Long, selfAsIri: Boolean)(implicit project: Project): RejOrResourceV[F] =
    EitherT
      .fromOptionF(repo.get(id, rev, None).value, notFound(id.ref, Some(rev)))
      .flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Fetches the provided tag of a resource
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, tag: String, selfAsIri: Boolean)(implicit project: Project): RejOrResourceV[F] =
    EitherT
      .fromOptionF(repo.get(id, tag, None).value, notFound(id.ref, tagOpt = Some(tag)))
      .flatMap(materializer.withMeta(_, selfAsIri))

  /**
    * Deprecates an existing resource
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long, schema: Ref)(implicit subject: Subject): RejOrResource[F] =
    for {
      _          <- repo.get(id, rev, Some(schema)).toRight(NotFound(id.ref, Some(rev)))
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

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
  final def apply[F[_]: Timer: Repo: ProjectResolution: Effect: Materializer](
      implicit config: AppConfig): Resources[F] =
    new Resources[F]()

  private[resources] final case class SchemaContext(schema: ResourceV,
                                                    dataImports: Set[ResourceV],
                                                    schemaImports: Set[ResourceV])

  private def replaceBNode(bnode: BNode, id: AbsoluteIri, value: ResourceF.Value): RootedGraph =
    RootedGraph(id, value.graph.replaceNode(bnode, id))

  private def rootNode(value: ResourceF.Value): Option[IriOrBNode] = {
    val resolvedSource = value.source appendContextOf Json.obj("@context" -> value.ctx)
    resolvedSource.id.map(IriNode.apply) orElse
      (value.graph: Graph).rootNode orElse
      (if (value.graph.triples.isEmpty) Some(blank) else None)
  }

  private[resources] def checkId[F[_]: Applicative](id: ResId,
                                                    value: ResourceF.Value): EitherT[F, Rejection, RootedGraph] =
    rootNode(value) match {
      case Some(IriNode(iri)) if iri.value == id.value =>
        EitherT.rightT[F, Rejection](RootedGraph(id.value, value.graph))
      case Some(bNode: BNode) =>
        EitherT.rightT[F, Rejection](replaceBNode(bNode, id.value, value))
      case _ =>
        EitherT.leftT[F, RootedGraph](IncorrectId(id.ref): Rejection)
    }

  private[resources] def checkOrAssignId[F[_]: Applicative](base: AbsoluteIri, value: ResourceF.Value)(
      implicit project: Project): EitherT[F, Rejection, (ResId, RootedGraph)] =
    rootNode(value) match {
      case Some(IriNode(iri)) =>
        EitherT.rightT[F, Rejection](Id(project.ref, iri.value) -> RootedGraph(iri, value.graph))
      case Some(bNode: BNode) =>
        val iri = generateId(base)
        EitherT.rightT[F, Rejection](Id(project.ref, iri.value) -> replaceBNode(bNode, iri, value))
      case _ =>
        EitherT.leftT[F, (ResId, RootedGraph)](UnableToSelectResourceId: Rejection)
    }

  private[resources] def generateId(base: AbsoluteIri): AbsoluteIri = url"${base.asString}${uuid()}"

}
