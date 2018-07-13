package ch.epfl.bluebrain.nexus.kg.search

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{nxv, rdf}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{BNode, IriOrBNode}
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import io.circe.{Encoder, Json}

object QueryResultEncoder {

  /**
    * Encoder for scored query results
    */
  implicit val scoredEncoder: Encoder[ScoredQueryResults[AbsoluteIri]] = Encoder.instance { results =>
    //not used, but without it Jena will embed everything in @graph
    val mainNode = url"http://localhost"
    val graph = Graph(
      (mainNode, nxv.total, Node.literal(results.total)),
      (mainNode, nxv.maxScore, Node.literal(results.maxScore))
    )

    addResults(graph, mainNode, results, queryResultToTriples)

  }

  /**
    * Encoder for unscored query results
    */
  implicit val unscoredEncoder: Encoder[UnscoredQueryResults[AbsoluteIri]] = Encoder.instance { results =>
    //not used, but without it Jena will embed everything in @graph
    val mainNode = url"http://localhost"
    val graph = Graph(
      (mainNode, nxv.total, Node.literal(results.total))
    )
    addResults(graph, mainNode, results, queryResultToTriples)
  }

  private def queryResultToTriples(result: QueryResult[AbsoluteIri], node: BNode): Set[Graph.Triple] = result match {
    case ScoredQueryResult(score, source) =>
      Set(
        (node, nxv.resultId, Node.literal(source.show)),
        (node, nxv.score, Node.literal(score))
      )
    case UnscoredQueryResult(source) =>
      Set(
        (node, nxv.resultId, Node.literal(source.show))
      )
  }

  private def addResults[A <: QueryResult[AbsoluteIri]](
      graph: Graph,
      mainNode: IriOrBNode,
      results: QueryResults[AbsoluteIri],
      triples: (QueryResult[AbsoluteIri], BNode) => Set[Graph.Triple]): Json = {
    val graphWithList = results.results match {
      case Nil => graph.add(mainNode, nxv.results, rdf.nil)
      case first :: rest =>
        val firstNode     = BNode()
        val firstListNode = BNode()

        val graphWithFirstElement = graph ++ Graph(triples(first, firstNode))
          .add(mainNode, nxv.results, firstListNode)
          .add(firstListNode, rdf.first, firstNode)

        val (graphWithoutListEnd, lastElement) = rest.foldLeft((graphWithFirstElement, firstListNode)) {
          case ((g, listNode), result) =>
            val resultNode   = BNode()
            val nextListNode = BNode()

            val graphWithNext = g ++ Graph(triples(result, resultNode))
              .add(listNode, rdf.rest, nextListNode)
              .add(nextListNode, rdf.first, resultNode)

            (graphWithNext, nextListNode)
        }
        graphWithoutListEnd.add(lastElement, rdf.rest, rdf.nil)
    }
    import ch.epfl.bluebrain.nexus.rdf.syntax.nexus._
    graphWithList
      .asJson(Contexts.searchCtx)
      .getOrElse(graphToJsonWithoutContext(graphWithList))
      .removeKeys("@context", "@id")
      .addContext(Contexts.searchCtxUri)
  }

  private def graphToJsonWithoutContext(graph: Graph): Json = {
    import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
    graph.asJson
  }

}
