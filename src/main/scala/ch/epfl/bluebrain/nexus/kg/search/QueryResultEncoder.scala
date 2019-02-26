package ch.epfl.bluebrain.nexus.kg.search

import cats.Id
import cats.instances.either._
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.commons.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtxUri, searchCtx, searchCtxUri}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node.blank
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import io.circe.{Encoder, Json}

object QueryResultEncoder {

  implicit def rootNode[A]: RootNode[QueryResults[A]] = _ => blank

  implicit def rootNodeQueryResult[A](implicit rootNode: RootNode[A]): RootNode[QueryResult[A]] =
    qr => rootNode(qr.source)

  implicit def graphEncoderQrs[A: RootNode](
      implicit enc: GraphEncoder[Id, QueryResult[A]]): GraphEncoder[Id, QueryResults[A]] = {
    implicit val rootNodeQr = rootNodeQueryResult[A]
    GraphEncoder {
      case (rootNode, ScoredQueryResults(total, max, results)) =>
        RootedGraph(
          rootNode,
          Graph((rootNode, nxv.total, total), (rootNode, nxv.maxScore, max)).add(rootNode, nxv.results, results))
      case (rootNode, UnscoredQueryResults(total, results)) =>
        RootedGraph(rootNode, Graph((rootNode, nxv.total, total): Triple).add(rootNode, nxv.results, results))
    }
  }

  implicit def graphEncoderQrsEither[A: RootNode](
      implicit enc: GraphEncoder[Id, QueryResult[A]]): GraphEncoder[EncoderResult, QueryResults[A]] =
    graphEncoderQrs[A].toEither

  def json[A](value: QueryResults[A], extraCtx: Json = Json.obj())(
      implicit enc: GraphEncoder[EncoderResult, QueryResults[A]],
      node: RootNode[QueryResults[A]]): DecoderResult[Json] =
    value
      .as[Json](searchCtx deepMerge extraCtx)
      .map(_.removeKeys("@context", "@id").addContext(searchCtxUri).addContext(resourceCtxUri))

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
