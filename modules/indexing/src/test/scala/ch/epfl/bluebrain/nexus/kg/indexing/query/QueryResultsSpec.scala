package ch.epfl.bluebrain.nexus.kg.indexing.query

import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults._
import org.scalatest.{Matchers, WordSpecLike}

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
  }

}
