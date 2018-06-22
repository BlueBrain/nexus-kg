package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.data.{EitherT, OptionT}
import cats.{Applicative, Monad}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryAttributes, BinaryDescription}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.validation.Validator
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{BNode, IriNode}
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.nexus._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri}
import io.circe.Json

/**
  * Resource operations.
  */
object Resources {

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
  def create[F[_]: Monad: Resolution](
      projectRef: ProjectRef,
      base: AbsoluteIri,
      schema: Ref,
      source: Json
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    // format: off
    for {
      rawValue       <- materialize(source)
      value          <- checkOrAssignId(Right((projectRef, base)), rawValue)
      (id, assigned)  = value
      resource       <- create(id, schema, assigned)
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
  def create[F[_]: Monad: Resolution](
      id: ResId,
      schema: Ref,
      source: Json
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    // format: off
    for {
      rawValue      <- materialize(source)
      value         <- checkOrAssignId(Left(id), rawValue)
      (_, assigned)  = value
      resource      <- create(id, schema, assigned)
    } yield resource
    // format: off

  /**
    * Creates a new resource.
    *
    * @param id     the id of the resource
    * @param schema a schema reference that constrains the resource
    * @param value  the resource representation in json-ld and graph formats
    */
  def create[F[_]: Monad: Resolution](
    id: ResId,
    schema: Ref,
    value: ResourceF.Value
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] = {
    val schacl   = nxv.ShaclSchema.value
    val ontology = nxv.OntologySchema.value

    def checkAndJoinTypes(types: Set[AbsoluteIri]): EitherT[F, Rejection, Set[AbsoluteIri]] =
      EitherT.fromEither(schema.iri match {
        case `schacl` if types.isEmpty || types.contains(nxv.Schema)     => Right(types + nxv.Schema)
        case `schacl`                                                    => Left(IncorrectTypes(id.ref, types))
        case `ontology` if types.isEmpty || types.contains(nxv.Ontology) => Right(types + nxv.Ontology)
        case `ontology`                                                  => Left(IncorrectTypes(id.ref, types))
        case _                                                           => Right(types)
      })

    // format: off
    for {
      resolved      <- schemaContext(schema)
      _             <- validate(resolved.schema, resolved.schemaImports, resolved.dataImports, value.graph)
      joinedTypes   <- checkAndJoinTypes(value.graph.primaryTypes.map(_.value))
      created       <- repo.create(id, schema, joinedTypes, value.source)
    } yield created
    // format: on
  }

  /**
    * Fetches the latest revision of a resource
    *
    * @param id the id of the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def get[F[_]](id: ResId)(implicit repo: Repo[F]): OptionT[F, Resource] =
    repo.get(id)

  /**
    * Fetches the provided revision of a resource
    *
    * @param id  the id of the resource
    * @param rev the revision of the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def get[F[_]](id: ResId, rev: Long)(implicit repo: Repo[F]): OptionT[F, Resource] =
    repo.get(id, rev)

  /**
    * Fetches the provided tag of a resource
    *
    * @param id  the id of the resource
    * @param tag the tag of the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def get[F[_]](id: ResId, tag: String)(implicit repo: Repo[F]): OptionT[F, Resource] =
    repo.get(id, tag)

  /**
    * Updates an existing resource.
    *
    * @param id              the id of the resource
    * @param rev             the last known revision of the resource
    * @param source          the new source representation in json-ld format
    * @return either a rejection or the updated resource in the F context
    */
  def update[F[_]: Monad: Resolution](
      id: ResId,
      rev: Long,
      source: Json
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    // format: off
    for {
      resource    <- get(id, rev).toRight(NotFound(id.ref))
      value       <- materialize[F](id, source)
      graph        = value.graph
      resolved    <- schemaContext(resource.schema)
      _           <- validate(resolved.schema, resolved.schemaImports, resolved.dataImports, graph)
      updated     <- repo.update(id, rev, graph.primaryTypes.map(_.value), source)
    } yield updated
    // format: on

  /**
    * Deprecates an existing resource
    *
    * @param id  the id of the resource
    * @param rev             the last known revision of the resource
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def deprecate[F[_]](
      id: ResId,
      rev: Long
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    repo.deprecate(id, rev)

  /**
    * Tags a resource. This operation aliases the provided ''targetRev'' with the  provided ''tag''.
    *
    * @param id        the id of the resource
    * @param rev       the last known revision of the resource
    * @param targetRev the revision that is being aliased with the provided ''tag''
    * @param tag       the tag of the alias for the provided ''rev''
    * @return Some(resource) in the F context when found and None in the F context when not found
    */
  def tag[F[_]](
      id: ResId,
      rev: Long,
      targetRev: Long,
      tag: String
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    repo.tag(id, rev, targetRev, tag)

  /**
    * Adds an attachment to a resource.
    *
    * @param id     the id of the resource
    * @param rev    the last known revision of the resource
    * @param attach the attachment description metadata
    * @param source the source of the attachment
    * @tparam In the storage input type
    * @return either a rejection or the new resource representation in the F context
    */
  def attach[F[_], In](
      id: ResId,
      rev: Long,
      attach: BinaryDescription,
      source: In
  )(implicit repo: Repo[F], identity: Identity, store: AttachmentStore[F, In, _]): EitherT[F, Rejection, Resource] =
    repo.attach(id, rev, attach, source)

  /**
    * Removes an attachment from a resource.
    *
    * @param id       the id of the resource
    * @param rev      the last known revision of the resource
    * @param filename the attachment filename
    * @return either a rejection or the new resource representation in the F context
    */
  def unattach[F[_]](
      id: ResId,
      rev: Long,
      filename: String
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    repo.unattach(id, rev, filename)

  /**
    * Attempts to stream the resource's attachment identified by the argument id and the filename.
    *
    * @param id       the id of the resource.
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[F[_], Out](
      id: ResId,
      filename: String
  )(implicit repo: Repo[F], store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    repo.getAttachment(id, filename)

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the revision and the filename.
    *
    * @param id       the id of the resource.
    * @param rev      the revision of the resource
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[F[_], Out](
      id: ResId,
      rev: Long,
      filename: String
  )(implicit repo: Repo[F], store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    repo.getAttachment(id, rev, filename)

  /**
    * Attempts to stream the resource's attachment identified by the argument id, the tag and the filename. The
    * tag is transformed into a revision value using the latest resource tag to revision mapping.
    *
    * @param id       the id of the resource.
    * @param tag      the tag of the resource
    * @param filename the filename of the attachment
    * @tparam Out the type for the output streaming of the attachment binary
    * @return the optional streamed attachment in the F context
    */
  def getAttachment[F[_], Out](
      id: ResId,
      tag: String,
      filename: String
  )(implicit repo: Repo[F], store: AttachmentStore[F, _, Out]): OptionT[F, (BinaryAttributes, Out)] =
    repo.getAttachment(id, tag, filename)

  private def schemaContext[F[_]: Monad: Resolution](schema: Ref): EitherT[F, Rejection, SchemaContext] = {
    def partition(set: Set[ResourceV]): (Set[ResourceV], Set[ResourceV]) =
      set.partition(_.isSchema)
    // format: off
    for {
      resolvedSchema                <- schema.resolveOr(NotFound)
      materializedSchema            <- materialize(resolvedSchema)
      importedResources             <- imports(materializedSchema)
      (schemaImports, dataImports)  = partition(importedResources)
    } yield SchemaContext(materializedSchema, dataImports, schemaImports)
    // format: on
  }

  /**
    * Materializes a json entity into a ResourceF.Value, flattening its context and producing a raw graph. While
    * flattening the context references are transitively resolved.
    *
    * @param source the source representation of the entity
    */
  def materialize[F[_]: Monad: Resolution](source: Json): EitherT[F, Rejection, ResourceF.Value] = {
    def contextValueOf(json: Json): Json =
      json.hcursor.downField("@context").focus.getOrElse(Json.obj())

    def flattenValue(refs: List[Ref], contextValue: Json): EitherT[F, Rejection, Json] =
      (contextValue.asString, contextValue.asArray, contextValue.asObject) match {
        case (Some(str), _, _) =>
          val nextRef = Iri.absolute(str).toOption.map(Ref.apply)
          for {
            next  <- EitherT.fromOption[F](nextRef, IllegalContextValue(refs))
            res   <- next.resolveOr(NotFound)
            value <- flattenValue(next :: refs, contextValueOf(res.value))
          } yield value
        case (_, Some(arr), _) =>
          import cats.implicits._
          val jsons = arr
            .traverse(j => flattenValue(refs, j).value)
            .map(_.sequence)
          EitherT(jsons).map(_.foldLeft(Json.obj())(_ deepMerge _))
        case (_, _, Some(_)) => EitherT.rightT(contextValue)
        case (_, _, _)       => EitherT.leftT(IllegalContextValue(refs))
      }

    def graphFor(flattenCtx: Json): Graph =
      (source deepMerge Json.obj("@context" -> flattenCtx)).asGraph

    flattenValue(Nil, contextValueOf(source))
      .map(ctx => Value(source, ctx, graphFor(ctx)))
  }

  /**
    * Materializes a json entity into a ResourceF.Value, flattening its context and producing a raw graph. While
    * flattening the context references are transitively resolved.
    *
    * @param id     the primary id of the entity
    * @param source the source representation of the entity
    */
  def materialize[F[_]: Monad: Resolution](id: ResId, source: Json): EitherT[F, Rejection, ResourceF.Value] =
    // format: off
    for {
      rawValue      <- materialize(source)
      value         <- checkOrAssignId(Left(id), rawValue)
      (_, assigned)  = value
    } yield assigned
    // format: on

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materialize[F[_]: Monad: Resolution](resource: Resource): EitherT[F, Rejection, ResourceV] =
    for {
      value <- materialize[F](resource.id, resource.value)
    } yield resource.map(_ => value)

  /**
    * Materializes a resource flattening its context and producing a graph that contains the additional type information
    * and the system generated metadata. While flattening the context references are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materializeWithMeta[F[_]: Monad: Resolution](resource: Resource): EitherT[F, Rejection, ResourceV] =
    for {
      resourceV <- materialize[F](resource)
      graph = resourceV.value.graph
      value = resourceV.value.copy(graph = graph ++ resourceV.metadata(_.iri) ++ resourceV.typeGraph)
    } yield resourceV.map(_ => value)

  /**
    * Transitively imports resources referenced by the primary node of the resource through ''owl:imports'' if the
    * resource has type ''owl:Ontology''.
    *
    * @param resource the resource for which imports are looked up
    */
  def imports[F[_]: Monad: Resolution](resource: ResourceV): EitherT[F, Rejection, Set[ResourceV]] = {
    import cats.implicits._
    def canImport(id: AbsoluteIri, g: Graph): Boolean =
      g.cursor(id)
        .downField(rdf.tpe)
        .values
        .flatMap { vs =>
          val set = vs.toSet
          Some(set.contains(nxv.Schema) || set.contains(owl.Ontology))
        }
        .getOrElse(false)

    def importsValues(id: AbsoluteIri, g: Graph): Set[Ref] =
      if (canImport(id, g))
        g.objects(IriNode(id), owl.imports).unorderedFoldMap {
          case IriNode(iri) => Set(Ref(iri))
          case _            => Set.empty
        } else Set.empty

    def lookup(current: Map[Ref, ResourceV], remaining: List[Ref]): EitherT[F, Rejection, Set[ResourceV]] = {
      def load(ref: Ref): EitherT[F, Rejection, (Ref, ResourceV)] =
        current
          .find(_._1 == ref)
          .map(tuple => EitherT.rightT[F, Rejection](tuple))
          .getOrElse(ref.resolveOr(NotFound).flatMap(r => materialize(r)).map(ref -> _))

      if (remaining.isEmpty) EitherT.rightT(current.values.toSet)
      else {
        val batch: EitherT[F, Rejection, List[(Ref, ResourceV)]] =
          remaining.traverse(ref => load(ref))

        batch.flatMap { list =>
          val nextRemaining: List[Ref] = list.flatMap {
            case (ref, res) => importsValues(ref.iri, res.value.graph).toList
          }
          val nextCurrent: Map[Ref, ResourceV] = current ++ list.toMap
          lookup(nextCurrent, nextRemaining)
        }
      }
    }
    lookup(Map.empty, importsValues(resource.id.value, resource.value.graph).toList)
  }

  /**
    * Validate data against a SHACL schema.
    *
    * @param schema        schema to validate against
    * @param schemaImports resolved schema imports
    * @param dataImports   resolved data imports
    * @param data          data to validate
    */
  def validate[F[_]: Applicative](schema: ResourceV,
                                  schemaImports: Set[ResourceV],
                                  dataImports: Set[ResourceV],
                                  data: Graph): EitherT[F, Rejection, Unit] = {
    val resolvedSchema = schemaImports.foldLeft(schema.value.graph)(_ ++ _.value.graph)
    val resolvedData   = dataImports.foldLeft(data)(_ ++ _.value.graph)
    val report         = Validator.validate(resolvedSchema, resolvedData)
    if (report.conforms) EitherT.rightT(())
    else EitherT.leftT(InvalidResource(schema.id.ref, report))
  }

  final private case class SchemaContext(schema: ResourceV, dataImports: Set[ResourceV], schemaImports: Set[ResourceV])

  private def checkOrAssignId[F[_]: Applicative](
      idOrGenInput: Either[ResId, (ProjectRef, AbsoluteIri)],
      value: ResourceF.Value
  ): EitherT[F, Rejection, (ResId, ResourceF.Value)] = {

    def replaceBNode(bnode: BNode, id: AbsoluteIri): ResourceF.Value =
      value.copy(graph = value.graph.replaceNode(bnode, id))

    def uuid(): String =
      UUID.randomUUID().toString.toLowerCase

    idOrGenInput match {
      case Left(id) =>
        if (value.graph.subjects().contains(id.value)) EitherT.rightT((id, value))
        else {
          val withIdOpt = value.graph.primaryBNode.map { bnode =>
            (id, replaceBNode(bnode, id.value))
          }
          EitherT.fromOption(withIdOpt, IncorrectId(id.ref))
        }
      case Right((projectRef, base)) =>
        val withIdOpt = value.graph.primaryNode map {
          case IriNode(iri) => (Id(projectRef, iri), value)
          case b: BNode =>
            val id = Id(projectRef, base + uuid())
            (id, replaceBNode(b, id.value))
        }
        EitherT.fromOption(withIdOpt, UnableToSelectResourceId())
    }
  }
}
