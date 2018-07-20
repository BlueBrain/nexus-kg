package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

/**
  * Constant vocabulary values
  */
object Vocabulary {

  /**
    * Nexus vocabulary.
    */
  object nxv {
    val base: Iri.AbsoluteIri = url"https://bluebrain.github.io/nexus/vocabulary".value

    val rev           = url"$base/rev"
    val tag           = url"$base/tag"
    val deprecated    = url"$base/deprecated"
    val createdAt     = url"$base/createdAt"
    val updatedAt     = url"$base/updatedAt"
    val createdBy     = url"$base/createdBy"
    val updatedBy     = url"$base/updatedBy"
    val constrainedBy = url"$base/constrainedBy"
    val isPartOf      = url"$base/isPartOf"
    val priority      = url"$base/priority"
    val uuid          = url"$base/uuid"
    val resourceTypes = url"$base/resourceTypes"
    val projects      = url"$base/projects"
    val identities    = url"$base/identities"
    val realm         = url"$base/realm"
    val sub           = url"$base/sub"
    val group         = url"$base/group"

    val total    = url"$base/total"
    val results  = url"$base/results"
    val resultId = url"$base/resultId"
    val maxScore = url"$base/maxScore"
    val score    = url"$base/score"

    val Schema           = url"$base/Schema"
    val Resource         = url"$base/Resource"
    val Ontology         = url"$base/Ontology"
    val Resolver         = url"$base/Resolver"
    val InProject        = url"$base/InProject"
    val InAccount        = url"$base/InAccount"
    val CrossProject     = url"$base/CrossProject"
    val View             = url"$base/View"
    val ElasticView      = url"$base/ElasticView"
    val SparqlView       = url"$base/SparqlView"
    val UserRef          = url"$base/UserRef"
    val GroupRef         = url"$base/GroupRef"
    val AuthenticatedRef = url"$base/AuthenticatedRef"
    val Anonymous        = url"$base/Anonymous"

  }
}
