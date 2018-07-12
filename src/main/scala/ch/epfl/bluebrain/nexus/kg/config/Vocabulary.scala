package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

/**
  * Constant vocabulary values
  */
object Vocabulary {

  /**
    * XML schema vocabulary.
    */
  object xsd {
    val base     = "http://www.w3.org/2001/XMLSchema"
    val dateTime = url"$base#dateTime"
  }

  /**
    * Nexus vocabulary.
    */
  object nxv {
    val base          = "https://bluebrain.github.io/nexus/vocabulary"
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
    val CrossProject     = url"$base/CrossProject"
    val View             = url"$base/View"
    val ElasticView      = url"$base/ElasticView"
    val SparqlView       = url"$base/SparqlView"
    val UserRef          = url"$base/UserRef"
    val GroupRef         = url"$base/GroupRef"
    val AuthenticatedRef = url"$base/AuthenticatedRef"
    val Anonymous        = url"$base/Anonymous"

  }

  /**
    * RDF syntax vocabulary.
    */
  object rdf {
    val base  = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
    val tpe   = url"$base#type"
    val first = url"$base#first"
    val rest  = url"$base#rest"
    val nil   = url"$base#nil"
  }

  /**
    * Owl vocabulary.
    */
  object owl {
    val base    = "http://www.w3.org/2002/07/owl"
    val imports = url"$base#imports"

    val Ontology = url"$base#Ontology"
  }

  implicit def toAbsoluteUri(iriNode: IriNode): AbsoluteIri = iriNode.value
}
