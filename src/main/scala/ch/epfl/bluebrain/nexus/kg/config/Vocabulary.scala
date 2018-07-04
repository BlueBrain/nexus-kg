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

    val Schema      = url"$base/Schema"
    val Ontology    = url"$base/Ontology"
    val Resolver    = url"$base/Resolver"
    val View        = url"$base/View"
    val ElasticView = url"$base/ElasticView"
    val SparqlView  = url"$base/SparqlView"

    val Anonymous = url"$base/Anonymous"

    val schemas                     = "https://bluebrain.github.io/nexus/schemas"
    val ShaclSchema: AbsoluteIri    = url"$schemas/schacl"
    val Resource: AbsoluteIri       = url"$schemas/resource"
    val OntologySchema: AbsoluteIri = url"$schemas/ontology"

  }

  /**
    * RDF syntax vocabulary.
    */
  object rdf {
    val base = "https://www.w3.org/1999/02/22-rdf-syntax-ns"
    val tpe  = url"$base#type"
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
