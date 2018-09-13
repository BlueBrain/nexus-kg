package ch.epfl.bluebrain.nexus.kg.search

import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import io.circe.{Encoder, Json}

object QueryResultEncoder {

  private val mainNode: IriOrBNode = blank

  implicit def encoderQrs[A](implicit enc: GraphEncoder[QueryResult[A]]): GraphEncoder[QueryResults[A]] = GraphEncoder {
    case ScoredQueryResults(total, max, results) =>
      mainNode -> Graph((mainNode, nxv.total, total), (mainNode, nxv.maxScore, max)).add(mainNode, nxv.results, results)
    case UnscoredQueryResults(total, results) =>
      mainNode -> Graph(((mainNode, nxv.total, total): Triple)).add(mainNode, nxv.results, results)
  }

  def qrsEncoder[A](extraCtx: Json)(implicit enc: GraphEncoder[QueryResults[A]]): Encoder[QueryResults[A]] =
    Encoder.instance(
      _.asJson(searchCtx deepMerge extraCtx)
        .removeKeys("@context", "@id")
        .addContext(searchCtxUri)
        .addContext(resourceCtxUri))

  implicit def qrsEncoder[A](implicit enc: GraphEncoder[QueryResults[A]]): Encoder[QueryResults[A]] =
    qrsEncoder(Json.obj())

  implicit def qrsEncoderJson: Encoder[QueryResults[Json]] = {
    implicit def qrEncoderJson: Encoder[QueryResult[Json]] = Encoder.instance {
      case UnscoredQueryResult(v) => v.removeKeys(nxv.originalSource.prefix)
      case ScoredQueryResult(score, v) =>
        v.removeKeys(nxv.originalSource.prefix) deepMerge Json.obj(nxv.score.prefix -> Json.fromFloatOrNull(score))
    }
    def json(total: Long, list: List[QueryResult[Json]]): Json =
      Json
        .obj(nxv.total.prefix -> Json.fromLong(total), nxv.results.prefix -> Json.arr(list.map(qrEncoderJson(_)): _*))
        .addContext(searchCtxUri)
        .addContext(resourceCtxUri)
    Encoder.instance {
      case UnscoredQueryResults(total, list) =>
        json(total, list)
      case ScoredQueryResults(total, maxScore, list) =>
        json(total, list) deepMerge Json.obj(nxv.maxScore.prefix -> Json.fromFloatOrNull(maxScore))
    }
  }
}
