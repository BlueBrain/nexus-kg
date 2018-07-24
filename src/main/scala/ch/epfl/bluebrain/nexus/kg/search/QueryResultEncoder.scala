package ch.epfl.bluebrain.nexus.kg.search

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.{Encoder, Json}

object QueryResultEncoder {

  //not used, but without it Jena will embed everything in @graph
  private val mainNode = url"http://localhost"

  implicit val encoderQr: GraphEncoder[QueryResult[AbsoluteIri]] = GraphEncoder {
    case UnscoredQueryResult(iri) =>
      val node  = blank
      val graph = Graph().add(node, nxv.resultId, iri.show)
      node -> graph
    case ScoredQueryResult(score, iri) =>
      val node  = blank
      val graph = Graph((node, nxv.score, score), (node, nxv.resultId, iri.show))
      node -> graph
  }

  def qrsEncoder[A](extraCtx: Json)(implicit enc: GraphEncoder[QueryResult[A]]): Encoder[QueryResults[A]] =
    Encoder.instance { qrs =>
      val g = qrs match {
        case ScoredQueryResults(total, max, results) =>
          Graph((mainNode, nxv.total, total), (mainNode, nxv.maxScore, max))
            .add(mainNode, nxv.results, results)
        case UnscoredQueryResults(total, results) =>
          Graph().add(mainNode, nxv.total, total).add(mainNode, nxv.results, results)
      }
      g.asJson(searchCtx deepMerge extraCtx, Some(mainNode))
        .getOrElse(g.asJson)
        .removeKeys("@context", "@id")
        .removeKeys("@context", "@id")
        .addContext(searchCtxUri)
        .addContext(resourceCtxUri)
    }

  implicit def qrsEncoder[A](implicit enc: GraphEncoder[QueryResult[A]]): Encoder[QueryResults[A]] =
    qrsEncoder(Json.obj())
}
