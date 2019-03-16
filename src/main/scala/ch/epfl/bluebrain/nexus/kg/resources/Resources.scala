package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.commons.shacl.{ShaclEngine, ValidationReport}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resolve.ProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources.SchemaContext
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileDescription
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.search.QueryBuilder._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.{Fetch, Save}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, BNode, IriNode, IriOrBNode}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri, RootedGraph}
import io.circe.Json
import org.apache.jena.rdf.model.Model

/**
  * Resource operations.
  */
class Resources[F[_]](implicit F: Effect[F], val repo: Repo[F], resolution: ProjectResolution[F], config: AppConfig) {
  self =>

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
                                                           project: Project,
                                                           additional: AdditionalValidation[F]): RejOrResource[F] =
    // format: off
    for {
      rawValue       <- materialize(schema, source)
      value          <- checkOrAssignId(base, rawValue)
      (id, graph)  = value
      resource       <- create(id, schema, rawValue.copy(graph = graph.removeMetadata))
    } yield resource
  // format: on

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param source the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(id: ResId, schema: Ref, source: Json)(implicit subject: Subject,
                                                   project: Project,
                                                   additional: AdditionalValidation[F]): RejOrResource[F] =
    for {
      assigned <- materialize(id, schema, source)
      resource <- create(id, schema, assigned)
    } yield resource

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
  def createFile[In](projectRef: ProjectRef,
                     base: AbsoluteIri,
                     storage: Storage,
                     fileDesc: FileDescription,
                     source: In)(implicit subject: Subject, saveStorage: Save[F, In]): RejOrResource[F] =
    createFile(Id(projectRef, generateId(base)), storage, fileDesc, source)

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
  def createFile[In](id: ResId, storage: Storage, fileDesc: FileDescription, source: In)(
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
  def updateFile[In](id: ResId, storage: Storage, rev: Long, fileDesc: FileDescription, source: In)(
      implicit subject: Subject,
      saveStorage: Save[F, In]): RejOrResource[F] =
    repo.updateFile(id, storage, rev, fileDesc, source)

  private def create(id: ResId, schema: Ref, value: ResourceF.Value)(
      implicit subject: Subject,
      project: Project,
      additional: AdditionalValidation[F]): RejOrResource[F] = {

    val schemaType  = addSchemaTypes(schema)
    val graph       = schemaType.map(tpe => value.graph + ((id.value, rdf.tpe, tpe): Triple)).getOrElse(value.graph)
    val joinedTypes = graph.types(id.value).map(_.value)
    for {
      _        <- validate(id, schema, graph)
      newValue <- additional(id, schema, joinedTypes, value.copy(graph = RootedGraph(id.value, graph)), 1L)
      created  <- repo.create(id, schema, joinedTypes, newValue.source)
    } yield created
  }

  /**
    * Fetches the latest revision of a resource
    *
    * @param id     the id of the resource
    * @param schema the schema reference that constrains the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, schema: Ref): RejOrResource[F] =
    fetch(id).check(schema)

  /**
    * Fetches the provided revision of a resource
    *
    * @param id     the id of the resource
    * @param rev    the revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, rev: Long, schema: Ref): RejOrResource[F] =
    fetch(id, rev).check(schema)

  /**
    * Fetches the provided tag of a resource
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String, schema: Ref): RejOrResource[F] =
    fetch(id, tag).check(schema)

  /**
    * Fetches the latest revision of a resource
    *
    * @param id     the id of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId): RejOrResource[F] =
    repo.get(id, None).toRight(NotFound(id.ref))

  /**
    * Fetches the provided revision of a resource
    *
    * @param id     the id of the resource
    * @param rev    the rev of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, rev: Long): RejOrResource[F] =
    repo.get(id, rev, None).toRight(NotFound(id.ref, revOpt = Some(rev)))

  /**
    * Fetches the provided tag of a resource
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @return Right(resource) in the F context when found and Left(NotFound) in the F context when not found
    */
  def fetch(id: ResId, tag: String): RejOrResource[F] =
    repo.get(id, tag, None).toRight(NotFound(id.ref, tagOpt = Some(tag)))

  /**
    * Fetches the latest revision of a resource tags.
    *
    * @param id     the id of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(tags) in the F context when found and None in the F context when not found
    */
  def fetchTags(id: ResId, schema: Ref): RejOrTags[F] =
    fetch(id, schema).map(_.tags.map { case (tag, tagRev) => Tag(tagRev, tag) }.toSet)

  /**
    * Fetches the provided revision of a resource tags.
    *
    * @param id     the id of the resource
    * @param rev    the revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(tags) in the F context when found and None in the F context when not found
    */
  def fetchTags(id: ResId, rev: Long, schema: Ref): RejOrTags[F] =
    fetch(id, rev, schema).map(_.tags.map { case (tag, tagRev) => Tag(tagRev, tag) }.toSet)

  /**
    * Fetches the provided tag of a resource tags.
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @param schema the schema reference that constrains the resource
    * @return Some(tags) in the F context when found and None in the F context when not found
    */
  def fetchTags(id: ResId, tag: String, schema: Ref): RejOrTags[F] =
    fetch(id, tag, schema).map(_.tags.map { case (tagValue, rev) => Tag(rev, tagValue) }.toSet)

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
                                                              project: Project,
                                                              additional: AdditionalValidation[F]): RejOrResource[F] =
    // format: off
    for {
      _           <- fetch(id, rev, schema)
      schemaType   = addSchemaTypes(schema)
      value       <- materialize(id, schema, source)
      graph        = schemaType.map(tpe => value.graph + ((id.value, rdf.tpe, tpe): Triple)).getOrElse(value.graph)
      _           <- validate(id, schema, graph)
      joinedTypes  = graph.types(id.value).map(_.value)
      newValue    <- additional(id, schema, joinedTypes, value.copy(graph = RootedGraph(id.value, graph)), rev + 1)
      updated     <- repo.update(id, rev, joinedTypes, newValue.source)
    } yield updated
    // format: on

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
      _          <- fetch(id, rev, schema)
      deprecated <- repo.deprecate(id, rev)
    } yield deprecated

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param schema the schema reference that constrains the resource
    * @param json   the json payload which contains the targetRev and the tag
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def tag(id: ResId, rev: Long, schema: Ref, json: Json)(implicit subject: Subject): RejOrResource[F] = {
    for {
      _      <- fetch(id, rev, schema)
      tag    <- EitherT.fromEither[F](Tag(id, json))
      tagged <- repo.tag(id, rev, tag.rev, tag.value)
    } yield tagged
  }

  private def addSchemaTypes(schemaRef: Ref): Option[AbsoluteIri] =
    schemaRef match {
      case `viewRef`     => Some(nxv.View.value)
      case `storageRef`  => Some(nxv.Storage.value)
      case `resolverRef` => Some(nxv.Resolver.value)
      case `shaclRef`    => Some(nxv.Schema.value)
      case _             => None
    }

  /**
    * Attempts to stream the file resource for the latest revision.
    *
    * @param id     the id of the resource
    * @param schema the schema reference that constrains the resource
    * @return the optional streamed file in the F context
    */
  def fetchFile[Out](id: ResId, schema: Ref)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetchFile(fetch(id, schema))

  /**
    * Attempts to stream the file resource with specific revision.
    *
    * @param id     the id of the resource
    * @param rev    the revision of the resource
    * @param schema the schema reference that constrains the resource
    * @return the optional streamed file in the F context
    */
  def fetchFile[Out: Fetch](id: ResId, rev: Long, schema: Ref)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetchFile(fetch(id, rev, schema))

  /**
    * Attempts to stream the file resource with specific tag. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id     the id of the resource
    * @param tag    the tag of the resource
    * @param schema the schema reference that constrains the resource
    * @return the optional streamed file in the F context
    */
  def fetchFile[Out: Fetch](id: ResId, tag: String, schema: Ref)(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    fetchFile(fetch(id, tag, schema))

  //TODO: FIX
  private def fetchFile[Out: Fetch](rejOrResource: RejOrResource[F])(implicit fetchStorage: Fetch[F, Out]): RejOrFile[F, Out] =
    rejOrResource.subflatMap(resource => resource.file.toRight(NotFound(resource.id.ref))).flatMapF {
      case (storage, attr) => storage.fetch.apply(attr).map(out => Right(storage, attr, out))
    }

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
    view
      .map(v => elasticSearch.search[Json](queryFor(params), Set(v.index))(pagination))
      .getOrElse(F.pure[JsonResults](UnscoredQueryResults(0L, List.empty)))
      .recoverWith {
        case ElasticClientError(status, body) =>
          F.raiseError[QueryResults[Json]](
            KgError.InternalError(s"ElasticSearch query failed with status '${status.value}' and body '$body'"))
      }

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved. If the provided context and resulting graph are empty, the parent project's base and
    * vocab settings are injected as the context in order to recompute the graph from the original JSON source.
    *
    * @param resource the resource to materialize
    */
  def materialize(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    for {
      value <- materialize(resource.id, resource.schema, resource.value)
    } yield resource.map(_ => value)

  /**
    * Materializes a resource flattening its context and producing a graph that contains the additional type information
    * and the system generated metadata. While flattening the context references are transitively resolved. If the
    * provided context and resulting graph are empty, the parent project's base and vocab settings are injected as the
    * context in order to recompute the graph from the original JSON source.
    *
    * @param resource the resource to materialize
    */
  def materializeWithMeta(resource: Resource, selfAsIri: Boolean = false)(
      implicit project: Project): RejOrResourceV[F] =
    for {
      resourceV <- materialize(resource)
      value = resourceV.value.copy(
        graph =
          RootedGraph(resourceV.value.graph.rootNode, resourceV.value.graph.triples ++ resourceV.metadata(selfAsIri)))
    } yield resourceV.map(_ => value)

  /**
    * Transitively imports resources referenced by the primary node of the resource through ''owl:imports'' if the
    * resource has type ''owl:Ontology''.
    *
    * @param resId the resource id for which imports are looked up
    * @param graph the resource graph for which imports are looked up
    */
  private def imports(resId: ResId, graph: Graph)(implicit project: Project): EitherT[F, Rejection, Set[ResourceV]] = {

    def importsValues(id: AbsoluteIri, g: Graph): Set[Ref] =
      g.objects(IriNode(id), owl.imports).unorderedFoldMap {
        case IriNode(iri) => Set(iri.ref)
        case _            => Set.empty
      }

    def lookup(current: Map[Ref, ResourceV], remaining: List[Ref]): EitherT[F, Rejection, Set[ResourceV]] = {
      def load(ref: Ref): EitherT[F, Rejection, (Ref, ResourceV)] =
        current
          .find(_._1 == ref)
          .map(tuple => EitherT.rightT[F, Rejection](tuple))
          .getOrElse(resolveOrNotFound(ref).flatMap(materialize).map(ref -> _))

      if (remaining.isEmpty) EitherT.rightT(current.values.toSet)
      else {
        val batch: EitherT[F, Rejection, List[(Ref, ResourceV)]] =
          remaining.traverse(load)

        batch.flatMap { list =>
          val nextRemaining: List[Ref] = list.flatMap {
            case (ref, res) => importsValues(ref.iri, res.value.graph).toList
          }
          val nextCurrent: Map[Ref, ResourceV] = current ++ list.toMap
          lookup(nextCurrent, nextRemaining)
        }
      }
    }

    lookup(Map.empty, importsValues(resId.value, graph).toList)
  }

  private def materialize(schema: Ref, source: Json)(
      implicit project: Project): EitherT[F, Rejection, ResourceF.Value] = {

    def flattenCtx(refs: List[Ref], contextValue: Json): EitherT[F, Rejection, Json] =
      (contextValue.asString, contextValue.asArray, contextValue.asObject) match {
        case (Some(str), _, _) =>
          val nextRef = Iri.absolute(str).toOption.map(Ref.apply)
          for {
            next  <- EitherT.fromOption[F](nextRef, IllegalContextValue(refs))
            res   <- resolveOrNotFound(next)
            value <- flattenCtx(next :: refs, res.value.contextValue)
          } yield value
        case (_, Some(arr), _) =>
          val jsons = arr
            .traverse(j => flattenCtx(refs, j).value)
            .map(_.sequence)
          EitherT(jsons).map(_.foldLeft(Json.obj())(_ deepMerge _))
        case (_, _, Some(_)) => EitherT.rightT(contextValue)
        case (_, _, _)       => EitherT.leftT(IllegalContextValue(refs))
      }

    flattenCtx(Nil, source.contextValue).flatMap { flattened =>
      val value = schema match {
        case `unconstrainedRef` if flattened == Json.obj() =>
          val ctx = Json.obj(
            "@base"  -> Json.fromString(project.base.asString),
            "@vocab" -> Json.fromString(project.vocab.asString)
          )
          source.deepMerge(Json.obj("@context" -> ctx)).asGraph(blank).map(Value(source, ctx, _))
        case _ =>
          source
            .deepMerge(Json.obj("@context" -> flattened))
            .asGraph(blank)
            .map(graph => Value(source, flattened, graph))
      }
      EitherT.fromEither[F](value).leftSemiflatMap(e => Rejection.fromMarshallingErr[F](e))
    }
  }

  private def materialize(id: ResId, schema: Ref, source: Json)(
      implicit project: Project): EitherT[F, Rejection, ResourceF.Value] =
    // format: off
    for {
      rawValue      <- materialize(schema, source)
      graph         <- checkId(id, rawValue.copy(graph = rawValue.graph.removeMetadata))
    } yield rawValue.copy(graph = graph)
  // format: on

  private def validate(resId: ResId, schema: Ref, data: Graph)(
      implicit project: Project): EitherT[F, Rejection, Unit] = {
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
      // format: off
      for {
        resolvedSchema                <- resolveOrNotFound(schema)
        materializedSchema            <- materialize(resolvedSchema)
        importedResources             <- imports(materializedSchema.id, materializedSchema.value.graph)
        (schemaImports, dataImports)  = partition(importedResources)
      } yield SchemaContext(materializedSchema, dataImports, schemaImports)
      // format: on

    schema.iri match {
      case `unconstrainedSchemaUri` => EitherT.rightT(())
      case `shaclSchemaUri` =>
        imports(resId, data).flatMap { resolved =>
          val resolvedSets = resolved.foldLeft(data.triples)(_ ++ _.value.graph.triples)
          val resolvedData = RootedGraph(blank, resolvedSets).as[Model]()
          toEitherT(ShaclEngine(resolvedData, reportDetails = true))
        }
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

  private def replaceBNode(bnode: BNode, id: AbsoluteIri, value: ResourceF.Value): RootedGraph =
    RootedGraph(id, value.graph.replaceNode(bnode, id))

  private def rootNode(value: ResourceF.Value): Option[IriOrBNode] = {
    val resolvedSource = value.source appendContextOf Json.obj("@context" -> value.ctx)
    resolvedSource.id.map(IriNode.apply) orElse
      (value.graph: Graph).rootNode orElse
      (if (value.graph.triples.isEmpty) Some(blank) else None)
  }

  private def checkId(id: ResId, value: ResourceF.Value): EitherT[F, Rejection, RootedGraph] =
    rootNode(value) match {
      case Some(IriNode(iri)) if iri.value == id.value => EitherT.rightT(RootedGraph(id.value, value.graph))
      case Some(bNode: BNode)                          => EitherT.rightT(replaceBNode(bNode, id.value, value))
      case _                                           => EitherT.leftT(IncorrectId(id.ref))
    }

  private def checkOrAssignId(base: AbsoluteIri, value: ResourceF.Value)(
      implicit project: Project): EitherT[F, Rejection, (ResId, RootedGraph)] =
    rootNode(value) match {
      case Some(IriNode(iri)) =>
        EitherT.rightT(Id(project.ref, iri.value) -> RootedGraph(iri, value.graph))
      case Some(bNode: BNode) =>
        val iri = generateId(base)
        EitherT.rightT(Id(project.ref, iri.value) -> replaceBNode(bNode, iri, value))
      case _ =>
        EitherT.leftT(UnableToSelectResourceId)
    }

  private def generateId(base: AbsoluteIri): AbsoluteIri = url"${base.asString}${uuid()}"

  private def resolveOrNotFound(ref: Ref)(implicit project: Project): EitherT[F, Rejection, Resource] =
    EitherT.fromOptionF(resolution(project.ref)(self).resolve(ref), NotFound(ref))

  private final implicit class ResourceSchemaSyntax(private val resourceF: RejOrResource[F]) {
    def check(schema: Ref): RejOrResource[F] =
      resourceF.subflatMap {
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
  final def apply[F[_]: Repo: ProjectResolution: Effect](implicit config: AppConfig): Resources[F] =
    new Resources[F]()

  private[resources] final case class SchemaContext(schema: ResourceV,
                                                    dataImports: Set[ResourceV],
                                                    schemaImports: Set[ResourceV])
}
