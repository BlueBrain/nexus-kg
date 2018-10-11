package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.instances.either._
import cats.instances.vector._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.shacl.topquadrant.{ShaclEngine, ValidationReport}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Contexts}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resolve.ProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources.AdditionalValidation._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.Resources.SchemaContext
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryAttributes, BinaryDescription}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.search.QueryBuilder
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{BNode, IriNode}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.jena._
import ch.epfl.bluebrain.nexus.rdf.syntax.nexus._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import ch.epfl.bluebrain.nexus.rdf.{Graph, GraphConfiguration, Iri}
import io.circe.Json

/**
  * Resource operations.
  */
class Resources[F[_]](implicit F: Monad[F], val repo: Repo[F], resolution: ProjectResolution[F], config: AppConfig) {
  self =>
  //TODO: If we need to cast well known types, we should find a better way to do it
  // on the rdf library side.
  private implicit val graphConfig = GraphConfiguration(castDateTypes = false)
  type RejOrResourceV = EitherT[F, Rejection, ResourceV]
  type RejOrResource  = EitherT[F, Rejection, Resource]
  type OptResource    = OptionT[F, Resource]
  type JsonResults    = QueryResults[Json]

  /**
    * Creates a new resource attempting to extract the id from the source. If a primary node of the resulting graph
    * is found:
    * <ul>
    *   <li>if it's an iri then its value will be used</li>
    *   <li>if it's a bnode a new iri will be generated using the base value</li>
    * </ul>
    *
    * @param projectRef reference for the project in which the resource is going to be created.
    * @param base       base used to generate new ids.
    * @param schema     a schema reference that constrains the resource
    * @param source     the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create(projectRef: ProjectRef, base: AbsoluteIri, schema: Ref, source: Json)(implicit identity: Identity,
                                                                                   additional: AdditionalValidation[F] =
                                                                                     pass): RejOrResource =
    // format: off
    for {
      rawValue       <- materialize(projectRef, source)
      value          <- checkOrAssignId(Right((projectRef, base)), rawValue)
      (id, assigned)  = value
      resource       <- create(id, schema, assigned.copy(graph = assigned.graph.removeMetadata(id.value)))
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
  def createWithId(id: ResId, schema: Ref, source: Json)(implicit identity: Identity,
                                                         additional: AdditionalValidation[F] = pass): RejOrResource =
    for {
      assigned <- materialize(id, source)
      resource <- create(id, schema, assigned)
    } yield resource

  private def create(id: ResId, schema: Ref, value: ResourceF.Value)(
      implicit identity: Identity,
      additional: AdditionalValidation[F]): RejOrResource = {

    def checkAndJoinTypes(types: Set[AbsoluteIri]): EitherT[F, Rejection, Set[AbsoluteIri]] =
      EitherT.fromEither(schema.iri match {
        case `shaclSchemaUri` if types.isEmpty || types.contains(nxv.Schema)      => Right(types + nxv.Schema)
        case `shaclSchemaUri`                                                     => Left(IncorrectTypes(id.ref, types))
        case `ontologySchemaUri` if types.isEmpty || types.contains(nxv.Ontology) => Right(types + nxv.Ontology)
        case `ontologySchemaUri`                                                  => Left(IncorrectTypes(id.ref, types))
        case _                                                                    => Right(types)
      })

    for {
      _           <- validate(id, schema, value.graph)
      joinedTypes <- checkAndJoinTypes(value.graph.types(id.value).map(_.value))
      newValue    <- additional(id, schema, joinedTypes, value, 1L)
      created     <- repo.create(id, schema, joinedTypes, newValue.source)
    } yield created
  }

  /**
    * Fetches the latest revision of a resource
    *
    * @param id        the id of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, schemaOpt: Option[Ref]): OptResource =
    checkSchema(schemaOpt, repo.get(id))

  /**
    * Fetches the provided revision of a resource
    *
    * @param id        the id of the resource
    * @param rev       the revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, rev: Long, schemaOpt: Option[Ref]): OptResource =
    checkSchema(schemaOpt, repo.get(id, rev))

  /**
    * Fetches the provided tag of a resource
    *
    * @param id        the id of the resource
    * @param tag       the tag of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def fetch(id: ResId, tag: String, schemaOpt: Option[Ref]): OptResource =
    checkSchema(schemaOpt, repo.get(id, tag))

  /**
    * Updates an existing resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param source    the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update(id: ResId, rev: Long, schemaOpt: Option[Ref], source: Json)(
      implicit identity: Identity,
      additional: AdditionalValidation[F] = pass): RejOrResource = {
    def checkSchema(res: Resource): EitherT[F, Rejection, Unit] = schemaOpt match {
      case Some(schema) if schema != res.schema => EitherT.leftT(NotFound(schema))
      case _                                    => EitherT.rightT(())
    }
    // format: off
    for {
      resource    <- fetch(id, rev, None).toRight(NotFound(id.ref))
      _           <- checkSchema(resource)
      value       <- materialize(id, source)
      graph        = value.graph
      _           <- validate(id, resource.schema, value.graph)
      joinedTypes  = graph.types(id.value).map(_.value)
      newValue    <- additional(id, resource.schema, joinedTypes, value, rev + 1)
      updated     <- repo.update(id, rev, joinedTypes, newValue.source)
    } yield updated
    // format: on
  }

  /**
    * Deprecates an existing resource
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate(id: ResId, rev: Long, schemaOpt: Option[Ref])(implicit identity: Identity): RejOrResource =
    checkSchema(id, schemaOpt)(repo.deprecate(id, rev))

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param json      the json payload which contains the targetRev and the tag
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def tag(id: ResId, rev: Long, schemaOpt: Option[Ref], json: Json)(implicit identity: Identity): RejOrResource = {
    val result = for {
      graph <- (json deepMerge Contexts.tagCtx).asGraph
      cursor = graph.cursor()
      revValue <- cursor.downField(nxv.rev).focus.as[Long]
      tagValue <- cursor.downField(nxv.tag).focus.as[String]
    } yield tag(id, rev, schemaOpt, revValue, tagValue)
    result match {
      case Right(v) => v
      case _        => EitherT.leftT(InvalidPayload(id.ref, "Both 'tag' and 'rev' fields must be present."))
    }
  }

  private def tag(id: ResId, rev: Long, schemaOpt: Option[Ref], targetRev: Long, tag: String)(
      implicit identity: Identity): RejOrResource =
    checkSchema(id, schemaOpt)(repo.tag(id, rev, targetRev, tag))

  /**
    * Adds an attachment to a resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param attach    the attachment description metadata
    * @param source    the source of the attachment
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def attach[In](id: ResId, rev: Long, schemaOpt: Option[Ref], attach: BinaryDescription, source: In)(
      implicit identity: Identity,
      store: AttachmentStore[F, In, _]): RejOrResource =
    checkSchema(id, schemaOpt)(repo.attach(id, rev, attach, source))

  /**
    * Removes an attachment from a resource.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the attachment filename
    * @return either a rejection or the new resource representation in the F context
    */
  def unattach(id: ResId, rev: Long, schemaOpt: Option[Ref], filename: String)(
      implicit identity: Identity): RejOrResource =
    checkSchema(id, schemaOpt)(repo.unattach(id, rev, filename))

  /**
    * Attempts to stream the resource's attachment identified by the argument id and the filename.
    *
    * @param id        the id of the resource.
    * @param filename  the filename of the attachment
    * @param schemaOpt optional schema reference that constrains the resource
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    checkSchemaAtt(id, schemaOpt)(repo.getAttachment(id, filename))

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the revision and the filename.
    *
    * @param id        the id of the resource.
    * @param rev       the revision of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, rev: Long, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    checkSchemaAtt(id, schemaOpt)(repo.getAttachment(id, rev, filename))

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the tag and the filename. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id        the id of the resource.
    * @param tag       the tag of the resource
    * @param schemaOpt optional schema reference that constrains the resource
    * @param filename  the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def fetchAttachment[Out](id: ResId, tag: String, schemaOpt: Option[Ref], filename: String)(
      implicit store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    checkSchemaAtt(id, schemaOpt)(repo.getAttachment(id, tag, filename))

  /**
    * Lists resources for the given project
    *
    * @param views      the list of views available for the current project
    * @param deprecated deprecation status of the resources
    * @param pagination pagination options
    * @param tc         typed HTTP client
    * @return search results in the F context
    */
  def list(views: Set[View], deprecated: Option[Boolean], pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elastic: ElasticClient[F]): F[JsonResults] =
    list(views, deprecated, None, pagination)

  /**
    * Lists resources for the given project and schema
    *
    * @param views      the list of views available for the current project
    * @param deprecated     deprecation status of the resources
    * @param schema         schema by which the resources are constrained
    * @param pagination     pagination options
    * @return               search results in the F context
    */
  def list(views: Set[View], deprecated: Option[Boolean], schema: AbsoluteIri, pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elastic: ElasticClient[F]): F[JsonResults] =
    list(views, deprecated, Some(schema), pagination)

  /**
    * Lists resources for the given project and schema
    *
    * @param views      the list of views available for the current project
    * @param deprecated deprecation status of the resources
    * @param schema     optional schema by which the resources are constrained
    * @param pagination pagination options
    * @return search results in the F context
    */
  private def list(views: Set[View], deprecated: Option[Boolean], schema: Option[AbsoluteIri], pagination: Pagination)(
      implicit tc: HttpClient[F, JsonResults],
      elastic: ElasticClient[F]): F[JsonResults] =
    views.collectFirst { case v: ElasticView if v.id == nxv.defaultElasticIndex.value => v } match {
      case Some(view) =>
        elastic.search(QueryBuilder.queryFor(deprecated, schema), Set(view.index))(pagination)
      case None =>
        F.pure(UnscoredQueryResults(0L, List.empty))
    }

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materialize(resource: Resource): RejOrResourceV =
    for {
      value <- materialize(resource.id, resource.value)
    } yield resource.map(_ => value)

  /**
    * Materializes a resource flattening its context and producing a graph that contains the additional type information
    * and the system generated metadata. While flattening the context references are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materializeWithMeta(resource: Resource)(implicit wrapped: LabeledProject): RejOrResourceV =
    for {
      resourceV <- materialize(resource)
      value = resourceV.value.copy(
        graph = Graph(resourceV.value.graph.triples ++ resourceV.metadata ++ resourceV.typeTriples))
    } yield resourceV.map(_ => value)

  /**
    * Transitively imports resources referenced by the primary node of the resource through ''owl:imports'' if the
    * resource has type ''owl:Ontology''.
    *
    * @param resId the resource id for which imports are looked up
    * @param graph the resource graph for which imports are looked up
    */
  private def imports(resId: ResId, graph: Graph): EitherT[F, Rejection, Set[ResourceV]] = {
    import cats.implicits._

    def importsValues(id: AbsoluteIri, g: Graph): Set[Ref] =
      g.objects(IriNode(id), owl.imports).unorderedFoldMap {
        case IriNode(iri) => Set(Ref(iri))
        case _            => Set.empty
      }

    def lookup(current: Map[Ref, ResourceV], remaining: List[Ref]): EitherT[F, Rejection, Set[ResourceV]] = {
      def load(ref: Ref): EitherT[F, Rejection, (Ref, ResourceV)] =
        current
          .find(_._1 == ref)
          .map(tuple => EitherT.rightT[F, Rejection](tuple))
          .getOrElse(ref.resolveOr(resId.parent)(NotFound).flatMap(materialize).map(ref -> _))

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

  private def materialize(projectRef: ProjectRef, source: Json): EitherT[F, Rejection, ResourceF.Value] = {

    def flattenValue(refs: List[Ref], contextValue: Json): EitherT[F, Rejection, Json] =
      (contextValue.asString, contextValue.asArray, contextValue.asObject) match {
        case (Some(str), _, _) =>
          val nextRef = Iri.absolute(str).toOption.map(Ref.apply)
          for {
            next  <- EitherT.fromOption[F](nextRef, IllegalContextValue(refs))
            res   <- next.resolveOr(projectRef)(NotFound)
            value <- flattenValue(next :: refs, res.value.contextValue)
          } yield value
        case (_, Some(arr), _) =>
          val jsons = arr
            .traverse(j => flattenValue(refs, j).value)
            .map(_.sequence)
          EitherT(jsons).map(_.foldLeft(Json.obj())(_ deepMerge _))
        case (_, _, Some(_)) => EitherT.rightT(contextValue)
        case (_, _, _)       => EitherT.leftT(IllegalContextValue(refs))
      }

    def graphFor(flattenCtx: Json): EitherT[F, Rejection, Graph] = {
      val eitherGraph = source
        .deepMerge(Json.obj("@context" -> flattenCtx))
        .asGraph
        .left
        .map(e => Rejection.fromJenaModelErr(e))
      EitherT.fromEither[F](eitherGraph)
    }

    for {
      ctx   <- flattenValue(Nil, source.contextValue)
      graph <- graphFor(ctx)
    } yield Value(source, ctx, graph)
  }

  private def materialize(id: ResId, source: Json): EitherT[F, Rejection, ResourceF.Value] =
    // format: off
    for {
      rawValue      <- materialize(id.parent, source)
      value         <- checkOrAssignId(Left(id), rawValue.copy(graph = rawValue.graph.removeMetadata(id.value)))
      (_, assigned)  = value
    } yield assigned
  // format: on

  private def checkSchemaAtt[Out](id: ResId, schemaOpt: Option[Ref])(
      op: => OptionT[F, (BinaryAttributes, Out)]): OptionT[F, (BinaryAttributes, Out)] =
    schemaOpt match {
      case Some(_) => fetch(id, schemaOpt).flatMap(_ => op)
      case _       => op
    }

  private def checkSchema(id: ResId, schemaOpt: Option[Ref])(op: => RejOrResource): RejOrResource =
    schemaOpt match {
      case Some(schema) => fetch(id, schemaOpt).toRight[Rejection](NotFound(schema)).flatMap(_ => op)
      case _            => op
    }

  private def checkSchema(schemaOpt: Option[Ref], resource: OptResource): OptResource =
    resource.flatMap { res =>
      schemaOpt match {
        case Some(schema) if schema != res.schema => OptionT.none[F, Resource]
        case _                                    => resource
      }
    }

  private def validate(resId: ResId, schema: Ref, data: Graph): EitherT[F, Rejection, Unit] = {
    def toEitherT(optReport: Option[ValidationReport]): EitherT[F, Rejection, Unit] =
      optReport match {
        case Some(r) if r.isValid() => EitherT.rightT(())
        case Some(r)                => EitherT.leftT(InvalidResource(schema, r))
        case _ =>
          EitherT.leftT(Unexpected(s"unexpected error while attempting to validate schema '${schema.iri.asString}'"))
      }

    def partition(set: Set[ResourceV]): (Set[ResourceV], Set[ResourceV]) =
      set.partition(_.isSchema)

    def schemaContext(): EitherT[F, Rejection, SchemaContext] =
      // format: off
      for {
        resolvedSchema                <- schema.resolveOr(resId.parent)(NotFound)
        materializedSchema            <- materialize(resolvedSchema)
        importedResources             <- imports(materializedSchema.id, materializedSchema.value.graph)
        (schemaImports, dataImports)  = partition(importedResources)
      } yield SchemaContext(materializedSchema, dataImports, schemaImports)
      // format: on

    schema.iri match {
      case `resourceSchemaUri` => EitherT.rightT(())
      case `shaclSchemaUri` =>
        imports(resId, data).flatMap { resolved =>
          val resolvedSets = resolved.foldLeft(data.triples)(_ ++ _.value.graph.triples)
          val resolvedData = Graph(resolvedSets).asJenaModel
          toEitherT(ShaclEngine(resolvedData, reportDetails = true))
        }
      case _ =>
        schemaContext().flatMap { resolved =>
          val resolvedSchemaSets =
            resolved.schemaImports.foldLeft(resolved.schema.value.graph.triples)(_ ++ _.value.graph.triples)
          val resolvedSchema   = Graph(resolvedSchemaSets).asJenaModel
          val resolvedDataSets = resolved.dataImports.foldLeft(data.triples)(_ ++ _.value.graph.triples)
          val resolvedData     = Graph(resolvedDataSets).asJenaModel
          toEitherT(ShaclEngine(resolvedData, resolvedSchema, validateShapes = false, reportDetails = true))
        }
    }
  }

  private def checkOrAssignId(idOrGenInput: Either[ResId, (ProjectRef, AbsoluteIri)],
                              value: ResourceF.Value): EitherT[F, Rejection, (ResId, ResourceF.Value)] = {

    def replaceBNode(bnode: BNode, id: AbsoluteIri): ResourceF.Value =
      value.copy(graph = value.graph.replaceNode(bnode, id))

    def uuid(): String =
      UUID.randomUUID().toString.toLowerCase

    idOrGenInput match {
      case Left(id) =>
        value.primaryNode match {
          case Some(IriNode(iri)) if iri.value == id.value => EitherT.rightT(id -> value)
          case Some(bNode: BNode)                          => EitherT.rightT(id -> replaceBNode(bNode, id.value))
          case _                                           => EitherT.leftT(IncorrectId(id.ref))
        }
      case Right((projectRef, base)) =>
        value.primaryNode match {
          case Some(IriNode(iri)) => EitherT.rightT(Id(projectRef, iri) -> value)
          case Some(bNode: BNode) =>
            val iri = base + uuid()
            EitherT.rightT(Id(projectRef, iri) -> replaceBNode(bNode, iri))
          case _ => EitherT.leftT(UnableToSelectResourceId)
        }
    }
  }

  private final implicit class RefSyntax(ref: Ref) {

    def resolveOr(projectRef: ProjectRef)(f: Ref => Rejection): EitherT[F, Rejection, Resource] =
      EitherT.fromOptionF(resolution(projectRef)(self).resolve(ref), f(ref))
  }

}

object Resources {

  /**
    * @param config the implicitly available application configuration
    * @tparam F the monadic effect type
    * @return a new [[Resources]] for the provided F type
    */
  final def apply[F[_]: Monad: Repo: ProjectResolution](implicit config: AppConfig): Resources[F] =
    new Resources[F]()

  private[resources] final case class SchemaContext(schema: ResourceV,
                                                    dataImports: Set[ResourceV],
                                                    schemaImports: Set[ResourceV])
}
