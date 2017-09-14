package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.FullTextSearchQueries._
import org.scalatest.{Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._

class FullTextSearchQueriesSpec extends WordSpecLike with Matchers {

  "A matchAllTerms" should {
    "construct the appropriate query" in {
      val term = "rand"
      val expected =
        s"""
           |PREFIX bds: <http://www.bigdata.com/rdf/search#>
           |SELECT DISTINCT ?total ?s ?maxscore ?score ?rank
           |WITH {
           |	SELECT DISTINCT ?s (max(?rsv) AS ?score) (max(?pos) AS ?rank)
           |	WHERE
           |	{
           |		?s ?matchedProperty ?matchedValue .
           |		?matchedValue bds:search "rand" .
           |		?matchedValue bds:relevance ?rsv .
           |		?matchedValue bds:rank ?pos .
           |		FILTER ( !isBlank(?s) )
           |	}
           |	GROUP BY ?s
           |} AS %resultSet
           |WHERE
           |{
           |	{ SELECT (COUNT(DISTINCT ?s) AS ?total) (max(?score) AS ?maxscore)
           |		WHERE
           |		{
           |			INCLUDE %resultSet
           |		}
           |	}
           |	UNION
           |	{
           |		SELECT *
           |		WHERE
           |		{
           |			INCLUDE %resultSet
           |		}
           |		LIMIT 100
           |	}
           |}
           |ORDER BY DESC(?score)
         """.stripMargin.trim
      matchAllTerms(subject, term, Pagination(0,100)).pretty shouldEqual expected
    }
  }
}
