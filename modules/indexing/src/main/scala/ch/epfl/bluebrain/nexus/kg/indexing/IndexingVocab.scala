package ch.epfl.bluebrain.nexus.kg.indexing

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._

/**
  * Defines the vocab used in SPARQL indexing process.
  */
trait IndexingVocab {

  /**
    * Uri vocabulary provided by W3C.
    */
  object PrefixUri {
    val rdf: Uri      = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val dateTime: Uri = "http://www.w3.org/2001/XMLSchema#dateTime"
  }

  /**
    * Indexing vocabulary, with prefix.
    */
  object PrefixMapping {
    val rdfTypeKey                                                                  = s"${PrefixUri.rdf}type"
    def createdAtTimeKey(implicit configuredQualifier: ConfiguredQualifier[String]) = "createdAtTime".qualifyAsString
    def updatedAtTimeKey(implicit configuredQualifier: ConfiguredQualifier[String]) = "updatedAtTime".qualifyAsString
    def publishedAtTimeKey(implicit configuredQualifier: ConfiguredQualifier[String]) =
      "publishedAtTime".qualifyAsString
    def readKey(implicit configuredQualifier: ConfiguredQualifier[String])         = "read".qualifyAsString
    def contextGroupKey(implicit configuredQualifier: ConfiguredQualifier[String]) = "contextGroup".qualifyAsString
    def schemaGroupKey(implicit configuredQualifier: ConfiguredQualifier[String])  = "schemaGroup".qualifyAsString
  }

  /**
    * JSON-LD keys used while indexing
    */
  object JsonLDKeys {
    val idKey      = "@id"
    val contextKey = "@context"
    val graphKey   = "@graph"
    val valueKey   = "@value"
    val typeKey    = "@value"
  }
}

object IndexingVocab extends IndexingVocab
