package ch.epfl.bluebrain.nexus.kg.indexing

import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import io.circe.{Encoder, Json}
import io.circe.parser.parse

/**
  * Encoders for [[View]]
  */
object ViewEncoder {

  implicit def qrViewEncoder: Encoder[QueryResults[View]] = {
    def addJsonMapping(result: Json): Json =
      result.hcursor
        .get[String]("mapping")
        .flatMap(parse)
        .map(mapping => result deepMerge Json.obj("mapping" -> mapping))
        .getOrElse(result)

    qrsEncoder[View](viewCtx mergeContext resourceCtx) mapJson { json =>
      val jsonWithCtx = json addContext viewCtxUri
      val results = jsonWithCtx.hcursor
        .downField("results")
        .focus
        .flatMap(_.asArray.map(_.map(addJsonMapping)))
      results.map(res => jsonWithCtx deepMerge Json.obj("results" -> Json.arr(res: _*))).getOrElse(jsonWithCtx)
    }
  }

  implicit val viewGraphEncoder: GraphEncoder[View] = GraphEncoder {
    case view @ ElasticView(mapping, resourceSchemas, resourceTag, includeMeta, sourceAsBlob, _, id, _, _, _) =>
      val s = IriNode(id)
      s -> Graph(
        view.mainTriples(nxv.ElasticView) ++ view
          .triplesFor(resourceSchemas) ++ view.triplesFor(includeMeta, sourceAsBlob, resourceTag, mapping))
    case view: SparqlView =>
      IriNode(view.id) -> Graph(view.mainTriples(nxv.SparqlView))
  }

  private implicit def qqViewEncoder(implicit enc: GraphEncoder[View]): GraphEncoder[QueryResult[View]] =
    GraphEncoder { res =>
      val encoded = enc(res.source)
      encoded.subject -> encoded.graph
    }

  private implicit class ViewSyntax(view: View) {
    private val s = IriNode(view.id)

    def mainTriples(tpe: AbsoluteIri): Set[Triple] =
      Set((s, rdf.tpe, nxv.View),
          (s, rdf.tpe, tpe),
          (s, nxv.uuid, view.uuid),
          (s, nxv.deprecated, view.deprecated),
          (s, nxv.rev, view.rev))

    def triplesFor(resourceSchemas: Set[AbsoluteIri]): Set[Triple] =
      resourceSchemas.map(r => (s: IriOrBNode, nxv.resourceSchemas, IriNode(r): Node))

    def triplesFor(includeMetadata: Boolean,
                   sourceAsBlob: Boolean,
                   resourceTagOpt: Option[String],
                   mapping: Json): Set[Triple] = {
      val triples = Set[Triple]((s, nxv.includeMetadata, includeMetadata),
                                (s, nxv.sourceAsBlob, sourceAsBlob),
                                (s, nxv.mapping, mapping.noSpaces))
      resourceTagOpt.map(resourceTag => triples + ((s, nxv.resourceTag, resourceTag))).getOrElse(triples)
    }

  }
}
