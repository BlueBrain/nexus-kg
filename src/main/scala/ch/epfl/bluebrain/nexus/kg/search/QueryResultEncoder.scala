package ch.epfl.bluebrain.nexus.kg.search

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.{Encoder, Json}

object QueryResultEncoder {

  /**
    * Encoder for scored query results
    */
  implicit val scoredEncoder: Encoder[ScoredQueryResults[AbsoluteIri]] = Encoder.instance { results =>
    Json
      .obj(
        "total"    -> Json.fromLong(results.total),
        "maxScore" -> Json.fromFloatOrString(results.maxScore),
        "results" -> Json.arr(
          results.results.map { res =>
            Json.obj(
              "resultId" -> Json.fromString(res.source.show)
            )

          }: _*
        )
      )
      .addContext(Contexts.searchCtxUri)
  }

  /**
    * Encoder for unscored query results
    */
  implicit val unscoredEncoder: Encoder[UnscoredQueryResults[AbsoluteIri]] = Encoder.instance { results =>
    Json
      .obj(
        "total" -> Json.fromLong(results.total),
        "results" -> Json.arr(
          results.results.map { res =>
            Json.obj(
              "resultId" -> Json.fromString(res.source.show)
            )

          }: _*
        )
      )
      .addContext(Contexts.searchCtxUri)
  }

}
