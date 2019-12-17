package ch.epfl.bluebrain.nexus.kg.indexing

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Authenticated, Group, User}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Source.{CrossProjectEventStream, ProjectEventStream}
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.{Projection, Source}
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.{Node, RootedGraph}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._

/**
  * Encoders for [[View]]
  */
object ViewEncoder {

  implicit val viewRootNode: RootNode[View] = v => IriNode(v.id)

  private def triples(rootNode: IriOrBNode, view: SparqlView) =
    filterTriples(rootNode, view.filter) + metadataTriple(rootNode, view.includeMetadata)

  private def triples(rootNode: IriOrBNode, view: ElasticSearchView) =
    Set[Triple](
      metadataTriple(rootNode, view.includeMetadata),
      (rootNode, nxv.mapping, view.mapping.noSpaces),
      (rootNode, nxv.sourceAsText, view.sourceAsText)
    ) ++ filterTriples(rootNode, view.filter)

  implicit def viewGraphEncoder(implicit config: IamClientConfig): GraphEncoder[Id, View] = GraphEncoder {
    case (rootNode, view: ElasticSearchView) =>
      RootedGraph(rootNode, triples(rootNode, view) ++ view.mainTriples(nxv.ElasticSearchView))

    case (rootNode, view: SparqlView) =>
      RootedGraph(rootNode, triples(rootNode, view) ++ view.mainTriples(nxv.SparqlView))

    case (rootNode, view: AggregateElasticSearchView) =>
      val triples = view.mainTriples(nxv.AggregateElasticSearchView) ++ view.triplesForView(view.value)
      RootedGraph(rootNode, triples)

    case (rootNode, view: AggregateSparqlView) =>
      val triples = view.mainTriples(nxv.AggregateSparqlView) ++ view.triplesForView(view.value)
      RootedGraph(rootNode, triples)

    case (rootNode, view @ CompositeView(sources, projections, rebuildStrategy, _, _, _, _, _)) =>
      val sourcesTriples = sources.foldLeft(Set.empty[Triple]) { (acc, source) =>
        val sourceCommon = sourceCommons(rootNode, source)
        source match {
          case ProjectEventStream(id, _, _, _) =>
            acc ++ sourceCommon + ((id, rdf.tpe, nxv.ProjectEventStream))
          case CrossProjectEventStream(id, _, _, _, projectIdentifier, identities) =>
            acc ++ sourceCommon ++ identitiesTriples(id, identities) + ((id, rdf.tpe, nxv.CrossProjectEventStream)) +
              ((id, nxv.project, projectIdentifier.show))
        }
      }
      val rebuildTriples = rebuildStrategy
        .map { interval =>
          val node = Node.blank
          Set[Triple](
            (rootNode, nxv.rebuildStrategy, node),
            (node, rdf.tpe, nxv.Interval),
            (node, nxv.value, interval.value.toString())
          )
        }
        .getOrElse(Set.empty[Triple])
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
      RootedGraph(
        rootNode,
        view.mainTriples(nxv.CompositeView) ++ sourcesTriples ++ projectionsTriples ++ rebuildTriples
      )
  }

  implicit def viewGraphEncoderEither(implicit config: IamClientConfig): GraphEncoder[EncoderResult, View] =
    viewGraphEncoder.toEither

  private def sourceCommons(s: IriOrBNode, source: Source): Set[Triple] =
    Set[Triple](
      (s, nxv.sources, source.id),
      (source.id, nxv.uuid, source.uuid.toString),
      metadataTriple(source.id, source.includeMetadata)
    ) ++
      filterTriples(source.id, source.filter)

  private def filterTriples(s: IriOrBNode, filter: Filter): Set[Triple] =
    filter.resourceSchemas.map(r => (s, nxv.resourceSchemas, r): Triple) ++
      filter.resourceTypes.map(r => (s, nxv.resourceTypes, r): Triple) +
      ((s, nxv.includeDeprecated, filter.includeDeprecated): Triple) ++
      filter.resourceTag.map(resourceTag => (s, nxv.resourceTag, resourceTag): Triple)

  private def metadataTriple(s: IriOrBNode, includeMetadata: Boolean): Triple =
    (s, nxv.includeMetadata, includeMetadata)

  def identitiesTriples(s: IriOrBNode, identities: List[Identity])(implicit config: IamClientConfig): Set[Triple] =
    identities.foldLeft(Set.empty[Triple]) { (acc, identity) =>
      val (identityId, triples) = identityTriples(identity)
      acc + ((s, nxv.identities, identityId)) ++ triples
    }

  private def identityTriples(identity: Identity)(implicit config: IamClientConfig): (IriNode, Set[Triple]) = {
    val ss = IriNode(identity.id)
    identity match {
      case User(sub, realm)     => ss -> Set((ss, rdf.tpe, nxv.User), (ss, nxv.realm, realm), (ss, nxv.subject, sub))
      case Group(group, realm)  => ss -> Set((ss, rdf.tpe, nxv.Group), (ss, nxv.realm, realm), (ss, nxv.group, group))
      case Authenticated(realm) => ss -> Set((ss, rdf.tpe, nxv.Authenticated), (ss, nxv.realm, realm))
      case _                    => ss -> Set((ss, rdf.tpe, nxv.Anonymous))
    }
  }

  private implicit class ViewSyntax(view: View) {
    private val s = IriNode(view.id)

    def mainTriples(tpe: AbsoluteIri*): Set[Triple] =
      Set[Triple](
        (s, rdf.tpe, nxv.View),
        (s, nxv.uuid, view.uuid.toString),
        (s, nxv.deprecated, view.deprecated),
        (s, nxv.rev, view.rev)
      ) ++ tpe.map(t => (s, rdf.tpe, t): Triple).toSet

    def triplesForView(views: Set[ViewRef]): Set[Triple] =
      views.flatMap { viewRef =>
        val ss = blank
        Set[Triple]((s, nxv.views, ss), (ss, nxv.viewId, viewRef.id), (ss, nxv.project, viewRef.project.show))
      }

  }
}
