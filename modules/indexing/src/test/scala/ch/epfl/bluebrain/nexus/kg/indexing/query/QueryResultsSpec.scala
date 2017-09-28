package ch.epfl.bluebrain.nexus.kg.indexing.query

import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults._
import io.circe.Json
import org.scalatest.{Matchers, WordSpecLike}
import io.circe.syntax._
import io.circe.generic.auto._
class QueryResultsSpec extends WordSpecLike with Matchers {

  "A QueryResults Functor" should {
    "transform the source and score values of the results" in {
      val qrs = ScoredQueryResults(1L,  1F, List(ScoredQueryResult(1F, 1)))
      qrs.map(_ + 1) shouldEqual ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, 2)))
    }

    "transform the score values of the results" in {
      val qrs = UnscoredQueryResults(1L,  List(UnscoredQueryResult(1)))
      qrs.map(_ + 1) shouldEqual UnscoredQueryResults(1L, List(UnscoredQueryResult(2)))
    }

    "transform the generic queryResults values" in {
      val qrs = UnscoredQueryResults(1L,  List(UnscoredQueryResult(1))) : QueryResults[Int]
      qrs.map(_ + 1) shouldEqual UnscoredQueryResults(1L, List(UnscoredQueryResult(2)))
    }

    "encodes a queryResults" in {
      val result = ScoredQueryResult(1F, 1): QueryResult[Int]
      val results = ScoredQueryResults(10L, 1F, List(result)): QueryResults[Int]
      results.asJson shouldEqual Json.obj(
        "total" -> Json.fromLong(results.total),
        "maxScore" -> Json.fromFloatOrNull(1F),
        "results" -> Json.arr(result.asJson)
      )
    }
  }

}
