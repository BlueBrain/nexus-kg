package ch.epfl.bluebrain.nexus.kg.service.query

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Query
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.ScoredQueryResults
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import io.circe.syntax._
import io.circe.{Encoder, Json}

/**
  * Data type which holds the SPARQL query response and adds HATEOAS links
  *
  * @param response the SPARQL query response
  * @param links    the links added to the response for
  *                 discovery purposes
  * @tparam A generic type of the response's payload
  */
final case class LinksQueryResults[A](response: QueryResults[A], links: Links)

object LinksQueryResults {

  /**
    * Constructs a [[LinksQueryResults]] from the SPARQL query response
    * and adds the correct links with the provided pagination.
    *
    * @param response   the SPARQL query response
    * @param pagination the pagination used to build the SPARQL query
    * @param uri        the endpoint used to make the SPARQL query
    * @tparam A generic type of the response's payload
    * @return an instance of [[LinksQueryResults]]
    */
  final def apply[A](response: QueryResults[A], pagination: Pagination, uri: Uri): LinksQueryResults[A] = {
    val self          = Links("self" -> uri)
    val responseCount = response.results.length

    def prevLink: Option[Links] = {
      if (response.total > responseCount && pagination.from > 0) {
        val size = Math.min(pagination.from, pagination.size.toLong)
        val from = Math.max(0, Math.min(pagination.from - pagination.size, response.total - size))
        val prev = uri.withQuery(addQuery(uri, from, size.toInt))
        Some(Links("previous" -> prev))
      } else None
    }

    def nextLink: Option[Links] = {
      if (response.total > responseCount && (pagination.from + responseCount) < response.total) {
        val next = uri.withQuery(addQuery(uri, pagination.size + pagination.from, pagination.size))
        Some(Links("next" -> next))
      } else None
    }

    LinksQueryResults(response, self ++ prevLink ++ nextLink)
  }

  final implicit def encodeLinksQueryResults[A](implicit qre: Encoder[QueryResult[A]],
                                                le: Encoder[Links]): Encoder[LinksQueryResults[A]] =
    Encoder.encodeJson.contramap { response =>
      val json = Json.obj(
        "total"   -> Json.fromLong(response.response.total),
        "results" -> response.response.results.asJson,
        "links"   -> response.links.asJson
      )
      response.response match {
        case ScoredQueryResults(_, maxScore, _) =>
          json deepMerge Json.obj("maxScore" -> Json.fromFloatOrNull(maxScore))
        case _ => json
      }
    }

  private def addQuery(uri: Uri, from: Long, size: Int): Query =
    Query(uri.query().toMap + ("from" -> s"$from", "size" -> s"$size"))
}
