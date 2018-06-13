package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{nxv, rdf}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import io.circe.Json

/**
  * A resource representation.
  *
  * @param id         the unique identifier of the resource
  * @param rev        the revision of the resource
  * @param types      the collection of known types of this resource
  * @param deprecated whether the resource is deprecated of not
  * @param tags       the collection of tag names to revisions of the resource
  * @param created    the instant when this resource was created
  * @param updated    the last instant when this resource was updated
  * @param createdBy  the identity that created this resource
  * @param updatedBy  the last identity that updated this resource
  * @param schema     the schema that this resource conforms to
  * @param value      the resource value
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
    *
    * @param f a schema representation to an iri mapping
    * @param parentF a parent representation to an [[ProjectRef]] mapping
    */
  def metadata(f: S => AbsoluteIri, parentF: P => ProjectRef): Graph = {
    val parent = parentF(id.parent)
    Graph(
      Set[Triple](
        (node, nxv.rev, Literal(rev)),
        (node, nxv.deprecated, false),
        (node, nxv.createdAt, created),
        (node, nxv.updatedAt, updated),
        (node, nxv.createdBy, createdBy),
        (node, nxv.updatedBy, updatedBy),
        (node, nxv.constrainedBy, f(schema)),
        (node, nxv.organization, parent.organization.id),
        (node, nxv.organization, parent.id)
      ))
  }

  /**
    * Computes the metadata graph for this resource.
    *
    * @param f  a schema representation to an iri mapping
    * @param ev the implicit evidence that the generic type [[P]]
    *           is equal to [[ProjectRef]]
    */
  def metadata(f: S => AbsoluteIri)(implicit ev: P =:= ProjectRef): Graph =
    metadata(f, ev.apply)

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
  final case class Value(source: Json, ctx: Json, graph: Graph)

}
