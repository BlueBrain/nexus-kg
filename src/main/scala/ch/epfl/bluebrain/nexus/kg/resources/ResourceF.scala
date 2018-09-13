package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant}

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{dcat, nxv}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, IriOrBNode}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.nexus._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import io.circe.Json

/**
  * A resource representation.
  *
  * @param id          the unique identifier of the resource
  * @param rev         the revision of the resource
  * @param types       the collection of known types of this resource
  * @param deprecated  whether the resource is deprecated of not
  * @param tags        the collection of tag names to revisions of the resource
  * @param attachments the collection of attachments
  * @param created     the instant when this resource was created
  * @param updated     the last instant when this resource was updated
  * @param createdBy   the identity that created this resource
  * @param updatedBy   the last identity that updated this resource
  * @param schema      the schema that this resource conforms to
  * @param value       the resource value
  * @tparam P the parent type of the resource identifier
  * @tparam S the schema type
  * @tparam A the resource value type
  */
final case class ResourceF[P, S, A](
    id: Id[P],
    rev: Long,
    types: Set[AbsoluteIri],
    deprecated: Boolean,
    tags: Map[String, Long],
    attachments: Set[BinaryAttributes],
    created: Instant,
    updated: Instant,
    createdBy: Identity,
    updatedBy: Identity,
    schema: S,
    value: A
) {

  /**
    * Applies the argument function to the resource value yielding a new resource.
    *
    * @param f the value mapping
    * @tparam B the output type of the mapping
    * @return a new resource with a mapped value
    */
  def map[B](f: A => B): ResourceF[P, S, B] =
    copy(value = f(value))

  /**
    * An IriNode for the @id of the resource.
    */
  lazy val node: IriNode = IriNode(id.value)

  /**
    * Computes the metadata triples for this resource.
    */
  def metadata(implicit config: AppConfig, wrapped: LabeledProject, ev: S =:= Ref): Set[Triple] = {

    def triplesFor(at: BinaryAttributes): Set[Triple] = {
      val blank       = Node.blank
      val blankDigest = Node.blank
      Set(
        (blankDigest, nxv.algorithm, at.digest.algorithm),
        (blankDigest, nxv.value, at.digest.value),
        (blank, rdf.tpe, dcat.Distribution),
        (blank, dcat.byteSize, at.byteSize),
        (blank, nxv.digest, blankDigest),
        (blank, dcat.mediaType, at.mediaType),
        (blank, nxv.originalFileName, at.filename),
        (blank, dcat.downloadURL, AccessId(id.value, schema.iri) + "attachments" + at.filename),
        (IriNode(id.value), dcat.distribution, blank)
      )
    }
    val schemaIri = ev(schema).iri
    Set[Triple](
      (node, nxv.rev, rev),
      (node, nxv.deprecated, deprecated),
      (node, nxv.createdAt, created),
      (node, nxv.updatedAt, updated),
      (node, nxv.createdBy, createdBy.id),
      (node, nxv.updatedBy, updatedBy.id),
      (node, nxv.constrainedBy, schemaIri),
      (node, nxv.project, wrapped.label.projectAccessId),
      (node, nxv.self, AccessId(id.value, schemaIri))
    ) ++ attachments.flatMap(triplesFor)
  }

  /**
    * The triples for the type of this resource.
    */
  lazy val typeTriples: Set[Triple] = types.map(tpe => (node, rdf.tpe, tpe): Triple)
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
    * Removes the metadata triples from the graph centered on the provided subject ''id''
    *
    * @param id the subject
    * @return a new [[Graph]] without the metadata triples
    */
  def removeMetadata(graph: Graph, id: IriOrBNode): Graph =
    graph.remove(id, metaPredicates.contains)

  /**
    * A default resource value type.
    *
    * @param source the source value of a resource
    * @param ctx    an expanded (flattened) context value
    * @param graph  a graph representation of a resource
    */
  final case class Value(source: Json, ctx: Json, graph: Graph) {
    def primaryNode: Option[IriOrBNode] = {
      val resolvedSource = source appendContextOf Json.obj("@context" -> ctx)
      resolvedSource.id.map(IriNode(_)) orElse graph.primaryNode orElse Option(graph.triples.isEmpty).collect {
        case true => Node.blank
      }
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
    * @tparam P the parent type of the resource identifier
    */
  def simpleF[P](id: Id[P],
                 value: Json,
                 rev: Long = 1L,
                 types: Set[AbsoluteIri] = Set.empty,
                 deprecated: Boolean = false,
                 schema: Ref = Ref(resourceSchemaUri),
                 created: Identity = Anonymous,
                 updated: Identity = Anonymous)(implicit clock: Clock): ResourceF[P, Ref, Json] =
    ResourceF(id,
              rev,
              types,
              deprecated,
              Map.empty,
              Set.empty,
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
    * @tparam P the parent type of the resource identifier
    */
  def simpleV[P](id: Id[P],
                 value: Value,
                 rev: Long = 1L,
                 types: Set[AbsoluteIri] = Set.empty,
                 deprecated: Boolean = false,
                 schema: Ref = Ref(resourceSchemaUri),
                 created: Identity = Anonymous,
                 updated: Identity = Anonymous)(implicit clock: Clock = Clock.systemUTC): ResourceF[P, Ref, Value] =
    ResourceF(id,
              rev,
              types,
              deprecated,
              Map.empty,
              Set.empty,
              clock.instant(),
              clock.instant(),
              created,
              updated,
              schema,
              value)
}
