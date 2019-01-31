package ch.epfl.bluebrain.nexus.kg.indexing

import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import io.circe.parser.parse
import io.circe.{Encoder, Json}

/**
  * Encoders for [[View]]
  */
object ViewEncoder {

  /**
    * Attempts to find the key ''field'' on the top Json level and transform the string value to Json. This will work
    * E.g.: {"field": "{\"a\": \"b\"}"} will be converted to {"field": {"a": "b"}}
    *
    * @param json the json to be transformed
    */
  def transformToJson(json: Json, field: String): Json =
    json.hcursor
      .get[String](field)
      .flatMap(parse)
      .map(value => json deepMerge Json.obj(field -> value))
      .getOrElse(json)

  implicit def qrViewEncoder: Encoder[QueryResults[View]] =
    qrsEncoder[View](viewCtx mergeContext resourceCtx) mapJson { json =>
      val jsonWithCtx = json addContext viewCtxUri
      val results = jsonWithCtx.hcursor
        .downField(nxv.results.prefix)
        .focus
        .flatMap(_.asArray.map(_.map(json => transformToJson(json, nxv.mapping.prefix))))
      results.map(res => jsonWithCtx deepMerge Json.obj(nxv.results.prefix -> Json.arr(res: _*))).getOrElse(jsonWithCtx)
    }

  implicit val viewGraphEncoder: GraphEncoder[View] = GraphEncoder {
    case view @ ElasticSearchView(mapping, resourceSchemas, resourceTag, includeMeta, sourceAsText, _, id, _, _, _) =>
      val s = IriNode(id)
      s -> Graph(
        view.mainTriples(nxv.ElasticSearchView, nxv.Alpha) ++ view
          .triplesFor(resourceSchemas) ++ view.triplesFor(includeMeta, sourceAsText, resourceTag, mapping))
    case view: SparqlView =>
      IriNode(view.id) -> Graph(view.mainTriples(nxv.SparqlView))
    case view @ AggregateElasticSearchView(_, _, _, id, _, _) =>
      IriNode(id) -> Graph(
        view.mainTriples(nxv.AggregateElasticSearchView, nxv.Alpha) ++ view.triplesForView(view.valueString))

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
                  (s, nxv.uuid, view.uuid.toString),
                  (s, nxv.deprecated, view.deprecated),
                  (s, nxv.rev, view.rev)) ++ tpe.map(t => (s, rdf.tpe, t): Triple).toSet

    def triplesFor(resourceSchemas: Set[AbsoluteIri]): Set[Triple] =
      resourceSchemas.map(r => (s, nxv.resourceSchemas, IriNode(r)): Triple)

    def triplesForView(views: Set[ViewRef[String]]): Set[Triple] =
      views.flatMap { viewRef =>
        val ss = blank
        Set[Triple]((s, nxv.views, ss), (ss, nxv.viewId, viewRef.id), (ss, nxv.project, viewRef.project))
      }

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
