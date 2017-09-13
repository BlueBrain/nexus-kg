package ch.epfl.bluebrain.nexus.kg.indexing.query

import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.ScoredQueryResult
import io.circe.{Encoder, Json}
import io.circe.generic.auto._
import org.scalatest.{Matchers, WordSpecLike}
class QueryResultSpec extends WordSpecLike with Matchers {

  "A QueryResult Functor" should {
    implicit val queryResultEncoder: Encoder[ScoredQueryResult[Int]] =
      Encoder.encodeJson.contramap { qr =>
        Json.obj(
          "resultId" -> Json.fromString("/some/path"),
          "score"    -> Json.fromFloatOrString(qr.score),
          "source"   -> Json.fromInt(qr.source),
        )
      }
    "transform the source value" in {
      ScoredQueryResult(1F, 1).map(_ + 1) shouldEqual ScoredQueryResult(1F, 2)
    }
    "encodes a queryResult" in {
      import io.circe.syntax._
      val result = ScoredQueryResult(1F, 1): QueryResult[Int]
      result.asJson shouldEqual Json.obj(
        "resultId" -> Json.fromString("/some/path"),
        "score"    -> Json.fromFloatOrString(1F),
        "source"   -> Json.fromInt(result.source),
      )
    }
  }

}
