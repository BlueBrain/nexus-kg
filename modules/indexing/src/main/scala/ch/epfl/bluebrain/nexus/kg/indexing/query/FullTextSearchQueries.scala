package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.TripleContent.{QueryContent, ValContent, VarContent}
import IndexingVocab.PrefixMapping._
import IndexingVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.Query

/**
  * Collection of full text search queries and commonly used constants.
  */
object FullTextSearchQueries {

  private val bdsSearchNamespace = "http://www.bigdata.com/rdf/search#"

  /**
    * SPARQL query builder that matches the argument ''term'' against all property values.
    *
    * @param subjectVar the subject variable to select
    * @param term       the term used to match against all property values of the specified subject
    * @param pagination the query pagination
    * @return the SPARQL query
    */
  final def matchAllTerms(subjectVar: String, term: String, pagination: Pagination): Query =
    QueryBuilder.prefix("bds" -> Uri(bdsSearchNamespace))
      .selectDistinct(subjectVar, "matchedProperty", score, rank, "GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',')" -> "groupedConcatenatedMatchedValue")
      .where((VarContent("matchedValue"), QueryContent(bdsSearch), ValContent(term)))
      .where((VarContent("matchedValue"), QueryContent(bdsRelevance), VarContent("score")))
      .where((VarContent("matchedValue"), QueryContent(bdsRank), VarContent("rank")))
      .where((VarContent(subject), VarContent("matchedProperty"), VarContent("matchedValue")))
      .filter("""!isBlank(?s)""")
      .pagination(pagination)
      .groupBy(subjectVar, "matchedProperty", score, rank).build()
}

/**
  * Defines a full text search query
  *
  * @param term       the search term
  * @param pagination the query pagination
  */
class FullTextSearchQuery(private val term: String, pagination: Pagination) {
  def build(): Query =
    FullTextSearchQueries.matchAllTerms(subject, term, pagination)
}

object FullTextSearchQuery {
  /**
    * Constructs a [[FullTextSearchQuery]]
    *
    * @param term       the search term
    * @param pagination the query pagination
    * @return an instance of [[FullTextSearchQuery]]
    */
  final def apply(term: String, pagination: Pagination): FullTextSearchQuery =
    new FullTextSearchQuery(term, pagination)
}