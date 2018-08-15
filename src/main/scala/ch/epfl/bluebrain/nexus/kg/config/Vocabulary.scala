package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

/**
  * Constant vocabulary values
  */
object Vocabulary {

  /**
    * Nexus vocabulary.
    */
  object nxv {
    val base: Iri.AbsoluteIri = url"https://bluebrain.github.io/nexus/vocabulary/".value

    /**
      * @param suffix the segment to add to the prefix mapping
      * @return an [[IriNode]] composed by the ''base'' plus the provided ''suffix''
      */
    def withPath(suffix: String): IriNode = IriNode(base + suffix)

    val rev                 = withPath("rev")
    val tag                 = withPath("tag")
    val deprecated          = withPath("deprecated")
    val createdAt           = withPath("createdAt")
    val updatedAt           = withPath("updatedAt")
    val createdBy           = withPath("createdBy")
    val updatedBy           = withPath("updatedBy")
    val constrainedBy       = withPath("constrainedBy")
    val isPartOf            = withPath("isPartOf")
    val priority            = withPath("priority")
    val uuid                = withPath("uuid")
    val resourceTypes       = withPath("resourceTypes")
    val resourceSchemas     = withPath("resourceSchemas")
    val resourceTag         = withPath("resourceTag")
    val includeMetadata     = withPath("includeMetadata")
    val sourceAsText        = withPath("sourceAsText")
    val mapping             = withPath("mapping")
    val projects            = withPath("projects")
    val identities          = withPath("identities")
    val realm               = withPath("realm")
    val sub                 = withPath("sub")
    val group               = withPath("group")
    val defaultElasticIndex = withPath("defaultElasticIndex")
    val defaultSparqlIndex  = withPath("defaultSparqlIndex")
    val originalSource      = withPath("_original_source")

    val total    = withPath("total")
    val results  = withPath("results")
    val resultId = withPath("resultId")
    val maxScore = withPath("maxScore")
    val score    = withPath("score")

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
}
