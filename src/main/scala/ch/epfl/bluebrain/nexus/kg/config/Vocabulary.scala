package ch.epfl.bluebrain.nexus.kg.config

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
    val deprecated    = url"$base/deprecated"
    val createdAt     = url"$base/createdAt"
    val updatedAt     = url"$base/updatedAt"
    val createdBy     = url"$base/createdBy"
    val updatedBy     = url"$base/updatedBy"
    val constrainedBy = url"$base/constrainedBy"
    val isPartOf      = url"$base/isPartOf"

    val Schema = url"$base/Schema"

    val Anonymous = url"$base/Anonymous"
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
}
