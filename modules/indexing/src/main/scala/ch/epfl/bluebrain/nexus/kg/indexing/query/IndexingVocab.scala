package ch.epfl.bluebrain.nexus.kg.indexing.query

/**
  * Defines the vocab used in SPARQL queries and in retrieving.
  * SPARQL response.
  */
trait IndexingVocab {

  /**
    * Vocabulary provided by Blazegraph, with bds prefix.
    */
  object PrefixMapping {
    val bdsSearch = "bds:search"
    val bdsRelevance = "bds:relevance"
    val bdsRank = "bds:rank"
  }

  /**
    * Terms used in the select block in SPARQL queries.
    */
  object SelectTerms {
    val score = "score"
    val rank = "rank"
    val subject = "s"
    val total = "total"
  }

}

object IndexingVocab extends IndexingVocab
