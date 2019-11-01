package ch.epfl.bluebrain.nexus.kg.indexing

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._

/**
  * Encoders for [[View]]
  */
object ViewEncoder {

  implicit val viewRootNode: RootNode[View] = v => IriNode(v.id)

  private def refsString(view: AggregateView[_]) = view.value match {
    case `Set[ViewRef[ProjectRef]]`(viewRefs)    => viewRefs.map(v => ViewRef(v.project.show, v.id))
    case `Set[ViewRef[ProjectLabel]]`(viewLabes) => viewLabes.map(v => ViewRef(v.project.show, v.id))
  }

  private def triples(rootNode: IriOrBNode, view: SparqlView) =
    filterTriples(rootNode, view.filter) + metadataTriple(rootNode, view.includeMetadata)

  private def triples(rootNode: IriOrBNode, view: ElasticSearchView) =
    Set[Triple](
      metadataTriple(rootNode, view.includeMetadata),
      (rootNode, nxv.mapping, view.mapping.noSpaces),
      (rootNode, nxv.sourceAsText, view.sourceAsText)
    ) ++ filterTriples(rootNode, view.filter)

  implicit val viewGraphEncoder: GraphEncoder[Id, View] = GraphEncoder {
    case (rootNode, view: ElasticSearchView) =>
      RootedGraph(rootNode, triples(rootNode, view) ++ view.mainTriples(nxv.ElasticSearchView))

    case (rootNode, view: SparqlView) =>
      RootedGraph(rootNode, triples(rootNode, view) ++ view.mainTriples(nxv.SparqlView))

    case (rootNode, view: AggregateElasticSearchView[_]) =>
      val triples = view.mainTriples(nxv.AggregateElasticSearchView) ++ view.triplesForView(refsString(view))
      RootedGraph(rootNode, triples)

    case (rootNode, view: AggregateSparqlView[_]) =>
      val triples = view.mainTriples(nxv.AggregateSparqlView) ++ view.triplesForView(refsString(view))
      RootedGraph(rootNode, triples)

    case (rootNode, view @ CompositeView(source, projections, _, _, _, _, _)) =>
      val sourceBNode = blank
      val sourceTriples = Set[Triple](
        (rootNode, nxv.sources, sourceBNode),
        (sourceBNode, rdf.tpe, nxv.ProjectEventStream)
      ) ++ filterTriples(sourceBNode, source.filter) + metadataTriple(sourceBNode, source.includeMetadata)
      val projectionsTriples = projections.flatMap {
        case Projection.ElasticSearchProjection(query, view, context) =>
          val node: IriNode = view.id
          Set[Triple](
            (rootNode, nxv.projections, node),
            (node, rdf.tpe, nxv.ElasticSearch),
            (node, nxv.query, query),
            (node, nxv.uuid, view.uuid.toString),
            (node, nxv.context, context.noSpaces)
          ) ++ triples(node, view)
        case Projection.SparqlProjection(query, view) =>
          val node: IriNode = view.id
          Set[Triple](
            (rootNode, nxv.projections, node),
            (node, rdf.tpe, nxv.Sparql),
            (node, nxv.query, query),
            (node, nxv.uuid, view.uuid.toString)
          ) ++ triples(node, view)
      }
      RootedGraph(rootNode, view.mainTriples(nxv.CompositeView) ++ sourceTriples ++ projectionsTriples)
  }

  implicit val viewGraphEncoderEither: GraphEncoder[EncoderResult, View] = viewGraphEncoder.toEither

  private def filterTriples(s: IriOrBNode, filter: Filter): Set[Triple] =
    filter.resourceSchemas.map(r => (s, nxv.resourceSchemas, r): Triple) ++
      filter.resourceTypes.map(r => (s, nxv.resourceTypes, r): Triple) +
      ((s, nxv.includeDeprecated, filter.includeDeprecated): Triple) ++
      filter.resourceTag.map(resourceTag => (s, nxv.resourceTag, resourceTag): Triple)

  private def metadataTriple(s: IriOrBNode, includeMetadata: Boolean): Triple =
    (s, nxv.includeMetadata, includeMetadata)

  private implicit class ViewSyntax(view: View) {
    private val s = IriNode(view.id)

    def mainTriples(tpe: AbsoluteIri*): Set[Triple] =
      Set[Triple](
        (s, rdf.tpe, nxv.View),
        (s, nxv.uuid, view.uuid.toString),
        (s, nxv.deprecated, view.deprecated),
        (s, nxv.rev, view.rev)
      ) ++ tpe.map(t => (s, rdf.tpe, t): Triple).toSet

    def triplesForView(views: Set[ViewRef[String]]): Set[Triple] =
      views.flatMap { viewRef =>
        val ss = blank
        Set[Triple]((s, nxv.views, ss), (ss, nxv.viewId, viewRef.id), (ss, nxv.project, viewRef.project))
      }

  }
}
