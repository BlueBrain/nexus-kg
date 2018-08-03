package ch.epfl.bluebrain.nexus.kg.resolve

import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{AuthenticatedRef, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import io.circe.Encoder

/**
  * Graph Encoder for [[Resolver]]
  */
object ResolverEncoder {

  implicit def qrResolverEncoder: Encoder[QueryResults[Resolver]] =
    qrsEncoder[Resolver](resolverCtx mergeContext resourceCtx) mapJson (_ addContext resolverCtxUri)

  implicit val resolverGraphEncoder: GraphEncoder[Resolver] = GraphEncoder {
    case resolver: InProjectResolver => IriNode(resolver.id) -> resolver.mainGraph(nxv.InProject)
    case resolver @ CrossProjectResolver(resourceTypes, projects, identities, _, id, _, _, _) =>
      val s             = IriNode(id)
      val projectsGraph = Graph(projects.map(r => (s: IriOrBNode, nxv.projects, r.id: Node)))
      s -> (resolver.mainGraph(nxv.CrossProject) ++ resolver.graphFor(identities) ++ resolver.graphFor(resourceTypes) ++ projectsGraph)
    case resolver @ InAccountResolver(resourceTypes, identities, _, _, id, _, _, _) =>
      val s = IriNode(id)
      s -> (resolver.mainGraph(nxv.InAccount) ++ resolver.graphFor(identities) ++ resolver.graphFor(resourceTypes))
  }

  private implicit def qqResolverEncoder(implicit enc: GraphEncoder[Resolver]): GraphEncoder[QueryResult[Resolver]] =
    GraphEncoder { res =>
      val encoded = enc(res.source)
      encoded.subject -> encoded.graph
    }

  private implicit class ResolverSyntax(resolver: Resolver) {
    private val s = IriNode(resolver.id)

    def mainGraph(tpe: AbsoluteIri): Graph = {
      Graph(
        (s, rdf.tpe, nxv.Resolver),
        (s, rdf.tpe, tpe),
        (s, nxv.priority, resolver.priority),
        (s, nxv.deprecated, resolver.deprecated),
        (s, nxv.rev, resolver.rev)
      )
    }

    def graphFor(identities: List[Identity]): Graph =
      identities.foldLeft(Graph()) { (finalGraph, identity) =>
        val (bNode, graph) = graphFor(identity)
        finalGraph + ((s, nxv.identities, bNode)) ++ graph
      }

    def graphFor(resourceTypes: Set[AbsoluteIri]): Graph =
      Graph(resourceTypes.map(r => (s: IriOrBNode, nxv.resourceTypes, IriNode(r): Node)))

    private def graphFor(identity: Identity): (BNode, Graph) = {
      val ss = blank
      identity match {
        case UserRef(realm, sub)           => ss -> Graph((ss, rdf.tpe, nxv.UserRef), (ss, nxv.realm, realm), (ss, nxv.sub, sub))
        case GroupRef(realm, g)            => ss -> Graph((ss, rdf.tpe, nxv.GroupRef), (ss, nxv.realm, realm), (ss, nxv.group, g))
        case AuthenticatedRef(Some(realm)) => ss -> Graph((ss, rdf.tpe, nxv.AuthenticatedRef), (ss, nxv.realm, realm))
        case AuthenticatedRef(_)           => ss -> Graph((ss, rdf.tpe, nxv.AuthenticatedRef))
        case _                             => ss -> Graph((ss, rdf.tpe, nxv.Anonymous))
      }
    }
  }
}
