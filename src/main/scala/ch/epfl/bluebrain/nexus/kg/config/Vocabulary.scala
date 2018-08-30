package ch.epfl.bluebrain.nexus.kg.config

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

/**
  * Constant vocabulary values
  */
object Vocabulary {

  object schema {
    val base: Iri.AbsoluteIri          = url"http://schema.org/".value
    private[Vocabulary] implicit val _ = IriNode(base)

    // Attachment metadata vocabulary
    val distribution = Metadata("distribution")
    val algorithm    = Metadata("algorithm")
    val contentSize  = Metadata("contentSize")
    val downloadURL  = Metadata("downloadURL")
    val accessURL    = Metadata("accessURL")
    val mediaType    = Metadata("mediaType")
    val unit         = Metadata("_unit", base + "unitCode")
    val value        = Metadata("value")
  }

  /**
    * Nexus vocabulary.
    */
  object nxv {
    val base: Iri.AbsoluteIri          = url"https://bluebrain.github.io/nexus/vocabulary/".value
    private[Vocabulary] implicit val _ = IriNode(base)

    /**
      * @param suffix the segment to suffix to the base
      * @return an [[IriNode]] composed by the ''base'' plus the provided ''suffix''
      */
    def withPath(suffix: String): IriNode = IriNode(base + suffix)

    // Metadata vocabulary
    val rev           = Metadata("rev")
    val deprecated    = Metadata("deprecated")
    val createdAt     = Metadata("createdAt")
    val updatedAt     = Metadata("updatedAt")
    val createdBy     = Metadata("createdBy")
    val updatedBy     = Metadata("updatedBy")
    val constrainedBy = Metadata("constrainedBy")
    val self          = Metadata("self")
    val project       = Metadata("project")
    val total         = Metadata("total")
    val results       = Metadata("results")
    val maxScore      = Metadata("maxScore")
    val score         = Metadata("score")

    // Attachment metadata vocabulary
    val originalFileName = Metadata("originalFileName")
    val digest           = Metadata("digest")

    // Elasticsearch sourceAsText predicate
    val originalSource = Metadata("original_source")

    // Tagging resource payload vocabulary
    val tag = withPath("tag")

    // Resolvers payload vocabulary
    val priority      = withPath("priority")
    val resourceTypes = withPath("resourceTypes")
    val projects      = withPath("projects")
    val identities    = withPath("identities")
    val realm         = withPath("realm")
    val sub           = withPath("sub")
    val group         = withPath("group")

    // View payload vocabulary
    val uuid            = withPath("uuid")
    val resourceSchemas = withPath("resourceSchemas")
    val resourceTag     = withPath("resourceTag")
    val includeMetadata = withPath("includeMetadata")
    val sourceAsText    = withPath("sourceAsText")
    val mapping         = withPath("mapping")

    // View default ids
    val defaultElasticIndex = withPath("defaultElasticIndex")
    val defaultSparqlIndex  = withPath("defaultSparqlIndex")

    // @type platform ids
    val Schema           = withPath("Schema")
    val Resource         = withPath("Resource")
    val Ontology         = withPath("Ontology")
    val Resolver         = withPath("Resolver")
    val InProject        = withPath("InProject")
    val InAccount        = withPath("InAccount")
    val CrossProject     = withPath("CrossProject")
    val View             = withPath("View")
    val ElasticView      = withPath("ElasticView")
    val SparqlView       = withPath("SparqlView")
    val UserRef          = withPath("UserRef")
    val GroupRef         = withPath("GroupRef")
    val AuthenticatedRef = withPath("AuthenticatedRef")
    val Anonymous        = withPath("Anonymous")
    val Alpha            = withPath("Alpha")
  }

  /**
    * Metadata vocabulary.
    *
    * @param prefix the prefix associated to this term, used in the Json-LD context
    * @param value  the fully expanded [[AbsoluteIri]] to what the ''prefix'' resolves
    */
  final case class Metadata(prefix: String, value: AbsoluteIri)

  object Metadata {

    /**
      * Constructs a [[Metadata]] vocabulary term from the given ''base'' and the provided ''lastSegment''.
      *
      * @param lastSegment the last segment to append to the ''base'' to build the metadata
      *                    vocabulary term
      */
    def apply(lastSegment: String)(implicit base: IriNode): Metadata =
      Metadata("_" + lastSegment, base.value + lastSegment)

    implicit def metadatataIri(m: Metadata): IriNode             = IriNode(m.value)
    implicit def metadatataAbsoluteIri(m: Metadata): AbsoluteIri = m.value
    implicit def metadataToIriF(p: Metadata): IriNode => Boolean = _ == IriNode(p.value)
    implicit val metadatataShow: Show[Metadata]                  = Show.show(_.value.show)
  }
}
