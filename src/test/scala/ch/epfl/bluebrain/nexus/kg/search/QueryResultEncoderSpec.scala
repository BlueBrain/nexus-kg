package ch.epfl.bluebrain.nexus.kg.search

import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{Matchers, WordSpecLike}

class QueryResultEncoderSpec extends WordSpecLike with Matchers with Resources {

  implicit val orderedKeys = OrderedKeys(List("@context", "total", "maxScore", "results", "resultId", "score", ""))
  "QueryResultsEncoder" should {
    "encode ScoredQueryResults" in {
      val results = ScoredQueryResults[AbsoluteIri](
        3,
        0.3f,
        List(
          ScoredQueryResult(0.3f, url"http://nexus.com/result1".value),
          ScoredQueryResult(0.2f, url"http://nexus.com/result2".value),
          ScoredQueryResult(0.1f, url"http://nexus.com/result3".value)
        )
      )

      QueryResultEncoder.scoredEncoder(results).sortKeys shouldEqual jsonContentOf("/search/scored-query-results.json")
    }
    "encode UnscoredQueryResults" in {
      val results = UnscoredQueryResults[AbsoluteIri](
        3,
        List(
          UnscoredQueryResult(url"http://nexus.com/result1".value),
          UnscoredQueryResult(url"http://nexus.com/result2".value),
          UnscoredQueryResult(url"http://nexus.com/result3".value)
        )
      )

      QueryResultEncoder.unscoredEncoder(results).sortKeys shouldEqual jsonContentOf(
        "/search/unscored-query-results.json")

    }
  }

}
