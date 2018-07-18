package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant}

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
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
    * Computes the metadata graph for this resource.
    */
  def metadata(implicit ev: S =:= Ref, iamConfig: IamConfig): Graph =
    Graph(
      Set[Triple](
        (node, nxv.rev, rev),
        (node, nxv.deprecated, deprecated),
        (node, nxv.createdAt, created),
        (node, nxv.updatedAt, updated),
        (node, nxv.createdBy, createdBy.id),
        (node, nxv.updatedBy, updatedBy.id),
        (node, nxv.constrainedBy, ev(schema).iri)
      ))

  /**
    * The type graph of this resource.
    */
  lazy val typeGraph: Graph = types.foldLeft(Graph()) {
    case (g, tpe) => g add (node, rdf.tpe, tpe)
  }
}

object ResourceF {

  /**
    * A default resource value type.
    *
    * @param source the source value of a resource
    * @param ctx    an expanded (flattened) context value
    * @param graph  a graph representation of a resource
    */
  final case class Value(source: Json, ctx: Json, graph: Graph) {
    def primaryNode: Option[IriOrBNode] =
      source.id.map(IriNode(_)) orElse graph.primaryNode orElse Option(graph.triples.isEmpty).collect {
        case true => Node.blank
      }

  }

  /**
    * Construct a [[ResourceF]] with default parameters
    *
    * @param id         the unique identifier of the resource
    * @param value      the [[Json]] resource value
    * @param rev        the revision of the resource
    * @param types       the collection of known types of this resource
    * @param deprecated whether the resource is deprecated of not
    * @param schema     the schema that this resource conforms to
    * @tparam P the parent type of the resource identifier
    */
  def simpleF[P](id: Id[P],
                 value: Json,
                 rev: Long = 1L,
                 types: Set[AbsoluteIri] = Set.empty,
                 deprecated: Boolean = false,
                 schema: Ref = Ref(resourceSchemaUri))(implicit clock: Clock): ResourceF[P, Ref, Json] =
    ResourceF(id,
              rev,
              types,
              deprecated,
              Map.empty,
              Set.empty,
              clock.instant(),
              clock.instant(),
              Anonymous,
              Anonymous,
              schema,
              value)

  /**
    * Construct a [[ResourceF]] with default parameters
    *
    * @param id         the unique identifier of the resource
    * @param value      the [[Value]] resource value
    * @param rev        the revision of the resource
    * @param types       the collection of known types of this resource
    * @param deprecated whether the resource is deprecated of not
    * @param schema     the schema that this resource conforms to
    * @tparam P the parent type of the resource identifier
    */
  def simpleV[P](id: Id[P],
                 value: Json,
                 rev: Long = 1L,
                 types: Set[AbsoluteIri] = Set.empty,
                 deprecated: Boolean = false,
                 schema: Ref = Ref(resourceSchemaUri))(implicit clock: Clock): ResourceF[P, Ref, Value] =
    ResourceF(
      id,
      rev,
      types,
      deprecated,
      Map.empty,
      Set.empty,
      clock.instant(),
      clock.instant(),
      Anonymous,
      Anonymous,
      schema,
      Value(value, value.contextValue, value.asGraph)
    )

}
