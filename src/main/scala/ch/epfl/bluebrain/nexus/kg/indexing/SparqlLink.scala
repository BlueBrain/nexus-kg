package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Instant

import cats.Id
import ch.epfl.bluebrain.nexus.commons.search.QueryResult
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults.Binding
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import ch.epfl.bluebrain.nexus.kg.resources.Ref
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}

import scala.util.Try

sealed trait SparqlLink {

  /**
    * @return the @id value of the resource
    */
  def id: AbsoluteIri

  /**
    * @return the predicate from where the link has been found
    */
  def property: AbsoluteIri

  def asResource: Option[SparqlResourceLink]

  def asExternal: Option[SparqlExternalLink]
}

object SparqlLink {

  /**
    * A link that represents a managed resource on the platform.
    *
    * @param id            the @id value of the resource
    * @param project       the @id of the project where the resource belongs
    * @param self          the access url for this resources
    * @param rev           the revision of the resource
    * @param types         the collection of types of this resource
    * @param deprecated    whether the resource is deprecated of not
    * @param created       the instant when this resource was created
    * @param updated       the last instant when this resource was updated
    * @param createdBy     the identity that created this resource
    * @param updatedBy     the last identity that updated this resource
    * @param constrainedBy the schema that this resource conforms to
    * @param property      the predicate from where the link has been found
    */
  final case class SparqlResourceLink(
      id: AbsoluteIri,
      project: AbsoluteIri,
      self: AbsoluteIri,
      rev: Long,
      types: Set[AbsoluteIri],
      deprecated: Boolean,
      created: Instant,
      updated: Instant,
      createdBy: AbsoluteIri,
      updatedBy: AbsoluteIri,
      constrainedBy: Ref,
      property: AbsoluteIri
  ) extends SparqlLink {
    def asResource = Some(this)
    def asExternal = None
  }

  object SparqlResourceLink {

    /**
      * Attempts to create a [[SparqlResourceLink]] from the given bindings and the previous types for the same resource
      *
      * @param bindings      the sparql result bindings
      * @param links         the externalLink representation for the current resource
      * @param previousTypes the previous found types for the same resource
      */
    def apply(bindings: Map[String, Binding],
              links: SparqlExternalLink,
              previousTypes: Set[AbsoluteIri]): Option[SparqlLink] = {
      val tpe = bindings.get("type").map(_.value).flatMap(Iri.absolute(_).toOption)
      for {
        project    <- bindings.get(nxv.project.prefix).map(_.value).flatMap(Iri.absolute(_).toOption)
        self       <- bindings.get(nxv.self.prefix).map(_.value).flatMap(Iri.absolute(_).toOption)
        rev        <- bindings.get(nxv.rev.prefix).map(_.value).flatMap(v => Try(v.toLong).toOption)
        deprecated <- bindings.get(nxv.deprecated.prefix).map(_.value).flatMap(v => Try(v.toBoolean).toOption)
        created    <- bindings.get(nxv.createdAt.prefix).map(_.value).flatMap(v => Try(Instant.parse(v)).toOption)
        updated    <- bindings.get(nxv.updatedAt.prefix).map(_.value).flatMap(v => Try(Instant.parse(v)).toOption)
        createdBy  <- bindings.get(nxv.createdBy.prefix).map(_.value).flatMap(Iri.absolute(_).toOption)
        updatedBy  <- bindings.get(nxv.updatedBy.prefix).map(_.value).flatMap(Iri.absolute(_).toOption)
        constrainedBy <- bindings
          .get(nxv.constrainedBy.prefix)
          .map(_.value)
          .flatMap(Iri.absolute(_).toOption.map(_.ref))
      } yield
      // format: off
        SparqlResourceLink(links.id, project, self, rev, previousTypes ++ tpe, deprecated, created, updated, createdBy, updatedBy, constrainedBy, links.property)
      // format: on
    }
  }

  /**
    * A link that represents an external resource out of the platform.
    *
    * @param id       the @id value of the resource
    * @param property the predicate from where the link has been found
    */
  final case class SparqlExternalLink(id: AbsoluteIri, property: AbsoluteIri) extends SparqlLink {
    def asResource = None
    def asExternal = Some(this)
  }

  object SparqlExternalLink {

    /**
      * Attempts to create a [[SparqlExternalLink]] from the given bindings
      *
      * @param bindings the sparql result bindings
      */
    def apply(bindings: Map[String, Binding]): Option[SparqlExternalLink] =
      for {
        id       <- bindings.get("s").map(_.value).flatMap(Iri.absolute(_).toOption)
        property <- bindings.get("property").map(_.value).flatMap(Iri.absolute(_).toOption)
      } yield SparqlExternalLink(id, property)
  }

  implicit val rootNodeLinks: RootNode[SparqlLink] = link => IriNode(link.id)

  implicit val linkGraphEncoder: GraphEncoder[Id, SparqlLink] =
    GraphEncoder {
      case (rootNode, SparqlExternalLink(_, property)) =>
        RootedGraph(rootNode, (rootNode, nxv.property, property): Triple)
      case (rootNode, SparqlResourceLink(_, project, self, rev, types, dep, c, u, cBy, uBy, schema, prop)) =>
        val triples = Set[Triple](
          (rootNode, nxv.property, prop),
          (rootNode, nxv.deprecated, dep),
          (rootNode, nxv.rev, rev),
          (rootNode, nxv.createdAt, c),
          (rootNode, nxv.updatedAt, u),
          (rootNode, nxv.createdBy, cBy.asString),
          (rootNode, nxv.updatedBy, uBy.asString),
          (rootNode, nxv.constrainedBy, schema.iri),
          (rootNode, nxv.project, project),
          (rootNode, nxv.self, self.asString)
        ) ++ types.map(tpe => (rootNode, rdf.tpe, tpe): Triple)
        RootedGraph(rootNode, triples)
    }

  implicit val linksGraphEncoderEither: GraphEncoder[EncoderResult, SparqlLink] =
    linkGraphEncoder.toEither

  implicit val qqLinksEncoder: GraphEncoder[Id, QueryResult[SparqlLink]] =
    GraphEncoder { (rootNode, res) =>
      linkGraphEncoder.apply(rootNode, res.source)
    }

  implicit val qqqLinksEncoder: GraphEncoder[EncoderResult, QueryResult[SparqlLink]] =
    qqLinksEncoder.toEither
}
