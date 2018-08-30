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

  object dcat {
    val base: Iri.AbsoluteIri          = url"http://www.w3.org/ns/dcat#".value
    private[Vocabulary] implicit val _ = IriNode(base)

    /**
      * @param suffix the segment to suffix to the base
      * @return an [[IriNode]] composed by the ''base'' plus the provided ''suffix''
      */
    def withSuffix(suffix: String): IriNode = url"${base.show + suffix}"

    // Attachment metadata vocabulary
    val distribution = Metadata("distribution")
    val byteSize     = Metadata("byteSize")
    val downloadURL  = Metadata("downloadURL")
    val accessURL    = Metadata("accessURL")
    val mediaType    = Metadata("mediaType")

    val Distribution = withSuffix("Distribution")
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
    def withSuffix(suffix: String): IriNode = IriNode(base + suffix)

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
    val algorithm        = Metadata("algorithm")
    val value            = Metadata("value")

    // Elasticsearch sourceAsText predicate
    val originalSource = Metadata("original_source")

    // Tagging resource payload vocabulary
    val tag = withSuffix("tag")

    // Resolvers payload vocabulary
    val priority      = withSuffix("priority")
    val resourceTypes = withSuffix("resourceTypes")
    val projects      = withSuffix("projects")
    val identities    = withSuffix("identities")
    val realm         = withSuffix("realm")
    val sub           = withSuffix("sub")
    val group         = withSuffix("group")

    // View payload vocabulary
    val uuid            = withSuffix("uuid")
    val resourceSchemas = withSuffix("resourceSchemas")
    val resourceTag     = withSuffix("resourceTag")
    val includeMetadata = withSuffix("includeMetadata")
    val sourceAsText    = withSuffix("sourceAsText")
    val mapping         = withSuffix("mapping")

    // View default ids
    val defaultElasticIndex = withSuffix("defaultElasticIndex")
    val defaultSparqlIndex  = withSuffix("defaultSparqlIndex")

    // @type platform ids
    val Schema           = withSuffix("Schema")
    val Resource         = withSuffix("Resource")
    val Ontology         = withSuffix("Ontology")
    val Resolver         = withSuffix("Resolver")
    val InProject        = withSuffix("InProject")
    val InAccount        = withSuffix("InAccount")
    val CrossProject     = withSuffix("CrossProject")
    val View             = withSuffix("View")
    val ElasticView      = withSuffix("ElasticView")
    val SparqlView       = withSuffix("SparqlView")
    val UserRef          = withSuffix("UserRef")
    val GroupRef         = withSuffix("GroupRef")
    val AuthenticatedRef = withSuffix("AuthenticatedRef")
    val Anonymous        = withSuffix("Anonymous")
    val Alpha            = withSuffix("Alpha")
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
      Metadata("_" + lastSegment, url"${base.value.show + lastSegment}".value)

    implicit def metadatataIri(m: Metadata): IriNode             = IriNode(m.value)
    implicit def metadatataAbsoluteIri(m: Metadata): AbsoluteIri = m.value
    implicit def metadataToIriF(p: Metadata): IriNode => Boolean = _ == IriNode(p.value)
    implicit val metadatataShow: Show[Metadata]                  = Show.show(_.value.show)
  }
}
