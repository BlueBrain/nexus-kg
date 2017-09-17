package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri

/**
  * Defines the vocab used in SPARQL queries and in retrieving.
  * SPARQL response.
  */
trait SearchVocab {

  /**
    * Vocabulary provided by Blazegraph, with bds prefix.
    */
  object PrefixMapping {
    val bdsSearch = "bds:search"
    val bdsRelevance = "bds:relevance"
    val bdsRank = "bds:rank"
  }

  /**
    * Uri vocabulary provided by Blazegraph.
    */
  object PrefixUri {
    val bdsUri = Uri("http://www.bigdata.com/rdf/search#")
  }

  /**
    * Terms used in the select block in SPARQL queries.
    */
  object SelectTerms {
    val score = "score"
    val rank = "rank"
    val subject = "s"
    val total = "total"
    val maxScore = "maxscore"
  }

}

object SearchVocab extends SearchVocab
