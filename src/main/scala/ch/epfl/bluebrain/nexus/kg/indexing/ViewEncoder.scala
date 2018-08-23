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

  /**
    * Attempts to find the key ''mapping'' on the top Json level and transform the string value to Json. This will work
    * E.g.: {"mapping": "{\"a\": \"b\"}"} will be converted to {"mapping": {"a": "b"}}
    *
    * @param json the json to be transformed
    */
  def transformToJson(json: Json): Json =
    json.hcursor
      .get[String]("mapping")
      .flatMap(parse)
      .map(mapping => json deepMerge Json.obj("mapping" -> mapping))
      .getOrElse(json)

  implicit def qrViewEncoder: Encoder[QueryResults[View]] =
    qrsEncoder[View](viewCtx mergeContext resourceCtx) mapJson { json =>
      val jsonWithCtx = json addContext viewCtxUri
      val results = jsonWithCtx.hcursor
        .downField("results")
        .focus
        .flatMap(_.asArray.map(_.map(transformToJson)))
      results.map(res => jsonWithCtx deepMerge Json.obj("results" -> Json.arr(res: _*))).getOrElse(jsonWithCtx)
    }

  implicit val viewGraphEncoder: GraphEncoder[View] = GraphEncoder {
    case view @ ElasticView(mapping, resourceSchemas, resourceTag, includeMeta, sourceAsText, _, id, _, _, _) =>
      val s = IriNode(id)
      s -> Graph(
        view.mainTriples(nxv.ElasticView, nxv.Alpha) ++ view
          .triplesFor(resourceSchemas) ++ view.triplesFor(includeMeta, sourceAsText, resourceTag, mapping))
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

    def mainTriples(tpe: AbsoluteIri*): Set[Triple] =
      Set[Triple]((s, rdf.tpe, nxv.View),
                  (s, nxv.uuid, view.uuid),
                  (s, nxv.deprecated, view.deprecated),
                  (s, nxv.rev, view.rev)) ++ tpe.map(t => (s, rdf.tpe, t): Triple).toSet

    def triplesFor(resourceSchemas: Set[AbsoluteIri]): Set[Triple] =
      resourceSchemas.map(r => (s: IriOrBNode, nxv.resourceSchemas, IriNode(r): Node))

    def triplesFor(includeMetadata: Boolean,
                   sourceAsText: Boolean,
                   resourceTagOpt: Option[String],
                   mapping: Json): Set[Triple] = {
      val triples = Set[Triple]((s, nxv.includeMetadata, includeMetadata),
                                (s, nxv.sourceAsText, sourceAsText),
                                (s, nxv.mapping, mapping.noSpaces))
      resourceTagOpt.map(resourceTag => triples + ((s, nxv.resourceTag, resourceTag))).getOrElse(triples)
    }

  }
}
