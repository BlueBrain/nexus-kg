package ch.epfl.bluebrain.nexus.kg.search

import java.util.regex.Pattern.quote

import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{Matchers, WordSpecLike}

class QueryResultEncoderSpec extends WordSpecLike with Matchers with Resources {

  implicit val orderedKeys = AppConfig.orderedKeys
  "QueryResultsEncoder" should {
    def json(id: AbsoluteIri): Json =
      jsonContentOf("/resources/es-metadata.json", Map(quote("{id}") -> id.asString)) deepMerge Json.obj(
        "_original_source" -> Json.fromString(Json.obj("k" -> Json.fromInt(1)).noSpaces))

    "encode ScoredQueryResults" in {
      val results: QueryResults[Json] = ScoredQueryResults[Json](
        3,
        0.3f,
        List(
          ScoredQueryResult(0.3f, json(url"http://nexus.com/result1".value)),
          ScoredQueryResult(0.2f, json(url"http://nexus.com/result2".value)),
          ScoredQueryResult(0.1f, json(url"http://nexus.com/result3".value))
        )
      )

      results.asJson.sortKeys shouldEqual jsonContentOf("/search/scored-query-results.json")
    }
    "encode UnscoredQueryResults" in {
      val results: QueryResults[Json] = UnscoredQueryResults[Json](
        3,
        List(
          UnscoredQueryResult(json(url"http://nexus.com/result1".value)),
          UnscoredQueryResult(json(url"http://nexus.com/result2".value)),
          UnscoredQueryResult(json(url"http://nexus.com/result3".value))
        )
      )

      results.asJson.sortKeys shouldEqual jsonContentOf("/search/unscored-query-results.json")

    }
  }

}
