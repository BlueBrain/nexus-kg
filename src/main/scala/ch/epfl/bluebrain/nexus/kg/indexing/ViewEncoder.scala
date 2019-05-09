package ch.epfl.bluebrain.nexus.kg.indexing

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import io.circe.parser.parse

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

  implicit val viewRootNode: RootNode[View] = v => IriNode(v.id)

  def json(qrsViews: QueryResults[View])(implicit enc: GraphEncoder[EncoderResult, QueryResults[View]],
                                         node: RootNode[QueryResults[View]]): DecoderResult[Json] =
    QueryResultEncoder.json(qrsViews, viewCtx mergeContext resourceCtx).map { json =>
      val jsonWithCtx = json addContext viewCtxUri
      val results = jsonWithCtx.hcursor
        .downField(nxv.results.prefix)
        .focus
        .flatMap(_.asArray.map(_.map(json => transformToJson(json, nxv.mapping.prefix))))
      results
        .map(res => jsonWithCtx deepMerge Json.obj(nxv.results.prefix -> Json.arr(res: _*)))
        .getOrElse(jsonWithCtx)
    }

  private def refsString(view: AggregateView[_]) = view.value match {
    case `Set[ViewRef[ProjectRef]]`(viewRefs)    => viewRefs.map(v => ViewRef(v.project.show, v.id))
    case `Set[ViewRef[ProjectLabel]]`(viewLabes) => viewLabes.map(v => ViewRef(v.project.show, v.id))
  }

  implicit val viewGraphEncoder: GraphEncoder[Id, View] = GraphEncoder {
    case (rootNode,
          view @ ElasticSearchView(map, schemas, types, tags, includeMeta, includeDep, sourceAsText, _, _, _, _, _)) =>
      val triples = view.mainTriples(nxv.ElasticSearchView) ++ view.triplesFor(schemas, types) ++
        view.triplesFor(includeMeta, includeDep, tags) ++ view.triplesFor(sourceAsText, map)
      RootedGraph(rootNode, triples)

    case (rootNode, view @ SparqlView(schemas, types, tags, includeMeta, includeDep, _, _, _, _, _)) =>
      val triples = view.mainTriples(nxv.SparqlView) ++ view.triplesFor(schemas, types) ++ view
        .triplesFor(includeMeta, includeDep, tags)
      RootedGraph(rootNode, triples)

    case (rootNode, view: AggregateElasticSearchView[_]) =>
      val triples = view.mainTriples(nxv.AggregateElasticSearchView) ++ view.triplesForView(refsString(view))
      RootedGraph(rootNode, triples)

    case (rootNode, view: AggregateSparqlView[_]) =>
      val triples = view.mainTriples(nxv.AggregateSparqlView) ++ view.triplesForView(refsString(view))
      RootedGraph(rootNode, triples)

  }

  implicit val viewGraphEncoderEither: GraphEncoder[EncoderResult, View] = viewGraphEncoder.toEither

  implicit def qqViewEncoder: GraphEncoder[Id, QueryResult[View]] =
    GraphEncoder { (rootNode, res) =>
      viewGraphEncoder(rootNode, res.source)
    }

  private implicit class ViewSyntax(view: View) {
    private val s = IriNode(view.id)

    def mainTriples(tpe: AbsoluteIri*): Set[Triple] =
      Set[Triple]((s, rdf.tpe, nxv.View),
                  (s, nxv.uuid, view.uuid.toString),
                  (s, nxv.deprecated, view.deprecated),
                  (s, nxv.rev, view.rev)) ++ tpe.map(t => (s, rdf.tpe, t): Triple).toSet

    def triplesFor(resourceSchemas: Set[AbsoluteIri], resourceTypes: Set[AbsoluteIri]): Set[Triple] =
      resourceSchemas.map(r => (s, nxv.resourceSchemas, r): Triple) ++
        resourceTypes.map(r => (s, nxv.resourceTypes, r): Triple)

    def triplesForView(views: Set[ViewRef[String]]): Set[Triple] =
      views.flatMap { viewRef =>
        val ss = blank
        Set[Triple]((s, nxv.views, ss), (ss, nxv.viewId, viewRef.id), (ss, nxv.project, viewRef.project))
      }

    def triplesFor(includeMetadata: Boolean,
                   includeDeprecated: Boolean,
                   resourceTagOpt: Option[String]): Set[Triple] = {
      val triple: Set[Triple] =
        Set((s, nxv.includeMetadata, includeMetadata), (s, nxv.includeDeprecated, includeDeprecated))
      resourceTagOpt.map(resourceTag => triple + ((s, nxv.resourceTag, resourceTag): Triple)).getOrElse(triple)
    }

    def triplesFor(sourceAsText: Boolean, mapping: Json): Set[Triple] = {
      Set[Triple]((s, nxv.sourceAsText, sourceAsText), (s, nxv.mapping, mapping.noSpaces))
    }

  }
}
