package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant}

import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Node, RootedGraph}
import io.circe.Json

/**
  * A resource representation.
  *
  * @param id         the unique identifier of the resource in a given project
  * @param rev        the revision of the resource
  * @param types      the collection of known types of this resource
  * @param deprecated whether the resource is deprecated of not
  * @param tags       the collection of tag names to revisions of the resource
  * @param file       the optional file metadata with the storage where the file was saved
  * @param created    the instant when this resource was created
  * @param updated    the last instant when this resource was updated
  * @param createdBy  the identity that created this resource
  * @param updatedBy  the last identity that updated this resource
  * @param schema     the schema that this resource conforms to
  * @param value      the resource value
  * @tparam A the resource value type
  */
final case class ResourceF[A](
    id: Id[ProjectRef],
    rev: Long,
    types: Set[AbsoluteIri],
    deprecated: Boolean,
    tags: Map[String, Long],
    file: Option[(Storage, FileAttributes)],
    created: Instant,
    updated: Instant,
    createdBy: Identity,
    updatedBy: Identity,
    schema: Ref,
    value: A
) {

  /**
    * Applies the argument function to the resource value yielding a new resource.
    *
    * @param f the value mapping
    * @tparam B the output type of the mapping
    * @return a new resource with a mapped value
    */
  def map[B](f: A => B): ResourceF[B] =
    copy(value = f(value))

  /**
    * An IriNode for the @id of the resource.
    */
  lazy val node: IriNode = IriNode(id.value)

  /**
    * Computes the metadata triples for this resource.
    */
  def metadata(implicit config: AppConfig, project: Project): Set[Triple] = {

    def triplesFor(storageAndAttributes: (Storage, FileAttributes)): Set[Triple] = {
      val blankDigest   = Node.blank
      val (storage, at) = storageAndAttributes
      Set(
        (blankDigest, nxv.algorithm, at.digest.algorithm),
        (blankDigest, nxv.value, at.digest.value),
        (node, rdf.tpe, nxv.File),
        (node, nxv.bytes, at.bytes),
        (node, nxv.digest, blankDigest),
        (node, nxv.mediaType, at.mediaType),
        (node, nxv.storageId, storage.id),
        (node, nxv.filename, at.filename)
      )
    }
    val fileTriples = file.map(triplesFor).getOrElse(Set.empty)
    val projectUri  = config.admin.publicIri + "projects" / project.organizationLabel / project.label
    val self        = AccessId(id.value, schema.iri)
    fileTriples + (
      (node, nxv.rev, rev),
      (node, nxv.deprecated, deprecated),
      (node, nxv.createdAt, created),
      (node, nxv.updatedAt, updated),
      (node, nxv.createdBy, createdBy.id),
      (node, nxv.updatedBy, updatedBy.id),
      (node, nxv.constrainedBy, schema.iri),
      (node, nxv.project, projectUri),
      (node, nxv.self, self.asString)
    ) ++ typeTriples
  }

  /**
    * The triples for the type of this resource.
    */
  private lazy val typeTriples: Set[Triple] = types.map(tpe => (node, rdf.tpe, tpe): Triple)

  /**
    * The context value (without @context) that gets injected to the HTTP response
    */
  def contextValueForJsonLd(implicit asValue: A =:= ResourceF.Value): Json = {
    val v = asValue(value)
    if (schema.iri == unconstrainedSchemaUri && v.source.contextValue == Json.obj()) v.ctx
    else v.source.contextValue
  }
}

object ResourceF {
  private val metaPredicates = Set[IriNode](nxv.rev,
                                            nxv.deprecated,
                                            nxv.createdAt,
                                            nxv.updatedAt,
                                            nxv.createdBy,
                                            nxv.updatedBy,
                                            nxv.constrainedBy,
                                            nxv.project,
                                            nxv.self)

  /**
    * Removes the metadata triples from the rooted graph
    *
    * @return a new [[RootedGraph]] without the metadata triples
    */
  def removeMetadata(rootedGraph: RootedGraph): RootedGraph =
    RootedGraph(rootedGraph.rootNode, rootedGraph.remove(rootedGraph.rootNode, metaPredicates.contains))

  /**
    * A default resource value type.
    *
    * @param source the source value of a resource
    * @param ctx    an expanded (flattened) context value
    * @param graph  a graph representation of a resource
    */
  final case class Value(source: Json, ctx: Json, graph: RootedGraph) {

    def map[A: RootNode](value: A, f: Json => Json)(implicit enc: GraphEncoder[CId, A]): Option[Value] = {
      val rootedGraph = value.asGraph
      val graphNoMeta = rootedGraph.removeMetadata
      val maybeJson   = graphNoMeta.as[Json](Json.obj("@context" -> ctx))
      maybeJson.toOption.map(json => copy(source = f(json), graph = graphNoMeta))
    }
  }

  /**
    * Construct a [[ResourceF]] with default parameters
    *
    * @param id         the unique identifier of the resource
    * @param value      the [[Json]] resource value
    * @param rev        the revision of the resource
    * @param types      the collection of known types of this resource
    * @param deprecated whether the resource is deprecated of not
    * @param schema     the schema that this resource conforms to
    * @param created    the identity that created this resource
    * @param updated    the last identity that updated this resource
    */
  def simpleF(id: Id[ProjectRef],
              value: Json,
              rev: Long = 1L,
              types: Set[AbsoluteIri] = Set.empty,
              deprecated: Boolean = false,
              schema: Ref = unconstrainedSchemaUri.ref,
              created: Identity = Anonymous,
              updated: Identity = Anonymous)(implicit clock: Clock): ResourceF[Json] =
    ResourceF(id,
              rev,
              types,
              deprecated,
              Map.empty,
              None,
              clock.instant(),
              clock.instant(),
              created,
              updated,
              schema,
              value)

  /**
    * Construct a [[ResourceF]] with default parameters
    *
    * @param id         the unique identifier of the resource
    * @param value      the [[Value]] resource value
    * @param rev        the revision of the resource
    * @param types      the collection of known types of this resource
    * @param deprecated whether the resource is deprecated of not
    * @param schema     the schema that this resource conforms to
    * @param created    the identity that created this resource
    * @param updated    the last identity that updated this resource
    */
  def simpleV(id: Id[ProjectRef],
              value: Value,
              rev: Long = 1L,
              types: Set[AbsoluteIri] = Set.empty,
              deprecated: Boolean = false,
              schema: Ref = unconstrainedSchemaUri.ref,
              created: Identity = Anonymous,
              updated: Identity = Anonymous)(implicit clock: Clock = Clock.systemUTC): ResourceF[Value] =
    ResourceF(id,
              rev,
              types,
              deprecated,
              Map.empty,
              None,
              clock.instant(),
              clock.instant(),
              created,
              updated,
              schema,
              value)
}
