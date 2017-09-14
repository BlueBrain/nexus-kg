package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.FullTextSearchQueries._
import org.scalatest.{Matchers, WordSpecLike}

class FullTextSearchQueriesSpec extends WordSpecLike with Matchers {

  "A matchAllTerms" should {
    "construct the appropriate query" in {
      val term = "rand"
      val expected =
        s"""
           |PREFIX bds: <http://www.bigdata.com/rdf/search#>
           |SELECT DISTINCT ?s ?matchedProperty ?score ?rank (GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',') AS ?groupedConcatenatedMatchedValue)
           |WHERE
           |{
           |	?matchedValue bds:search "$term" .
           |	?matchedValue bds:relevance ?score .
           |	?matchedValue bds:rank ?rank .
           |	?s ?matchedProperty ?matchedValue .
           |	FILTER ( !isBlank(?s) )
           |}
           |GROUP BY ?s ?matchedProperty ?score ?rank
           |LIMIT 100
        """.stripMargin.trim

      matchAllTerms("s", term, Pagination(0,100)).pretty shouldEqual expected
    }
  }
}
