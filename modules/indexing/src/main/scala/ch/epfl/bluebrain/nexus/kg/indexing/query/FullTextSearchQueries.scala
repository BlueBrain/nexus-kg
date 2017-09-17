package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.TripleContent.{QueryContent, ValContent, VarContent}
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixUri._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.Query
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Order.Desc
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field._
/**
  * Collection of full text search queries and commonly used constants.
  */
object FullTextSearchQueries {

  /**
    * SPARQL query builder that matches the argument ''term'' against all property values.
    *
    * @param subjectVar the subject variable to select
    * @param term       the term used to match against all property values of the specified subject
    * @param pagination the query pagination
    * @return the SPARQL query
    */
  final def matchAllTerms(subjectVar: String, term: String, pagination: Pagination): Query = {
    val withQuery =
      QueryBuilder.selectDistinct(subjectVar, "max(?rsv)" -> score, "max(?pos)" -> rank)
        .where((VarContent(subjectVar), VarContent("matchedProperty"), VarContent("matchedValue")))
        .where((VarContent("matchedValue"), QueryContent(bdsSearch), ValContent(term)))
        .where((VarContent("matchedValue"), QueryContent(bdsRelevance), VarContent("rsv")))
        .where((VarContent("matchedValue"), QueryContent(bdsRank), VarContent("pos")))
        .filter(s"!isBlank(?$subjectVar)")
        .groupBy(subjectVar)

    QueryBuilder.prefix("bds" -> bdsUri)
      .selectDistinct(total, subjectVar, maxScore, score, rank)
      .`with`(withQuery, "resultSet")
      .subQuery(QueryBuilder.select(count(), s"max(?$score)" -> maxScore).include("resultSet"))
      .union(QueryBuilder.selectAll.include("resultSet").pagination(pagination))
      .orderBy(Desc(score)).build()
  }
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