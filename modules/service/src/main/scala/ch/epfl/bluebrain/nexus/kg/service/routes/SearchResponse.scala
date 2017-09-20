package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives.{complete, extract, onSuccess}
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Encoder
import io.circe.generic.auto._

import scala.concurrent.Future

trait SearchResponse {

  /**
    * Syntactic sugar for constructing a [[Route]] from the [[QueryResults]]
    */
  implicit class QueryResultsOpts[A](qr: Future[QueryResults[A]]) {
    /**
      * Interface syntax to expose new functionality into [[QueryResults]] response type.
      *
      * @param base       the service public uri + prefix
      * @param pagination the pagination values
      */
    def buildResponse(base: Uri, pagination: Pagination)(implicit
      R: Encoder[UnscoredQueryResult[A]],
      S: Encoder[ScoredQueryResult[A]]): Route =
      extract(_.request.uri) { uri =>
        onSuccess(qr) { result =>
          val lqu = base.copy(path = uri.path, fragment = uri.fragment, rawQueryString = uri.rawQueryString)
          complete(StatusCodes.OK -> LinksQueryResults(result, pagination, lqu))
        }
      }
  }

}

object SearchResponse extends SearchResponse
