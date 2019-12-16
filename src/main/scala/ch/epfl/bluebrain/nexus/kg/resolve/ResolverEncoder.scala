package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import ch.epfl.bluebrain.nexus.rdf.Node.literal
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._

/**
  * Encoders for [[Resolver]]
  */
object ResolverEncoder {

  implicit val resolverRootNode: RootNode[Resolver] = r => IriNode(r.id)

  implicit def resolverGraphEncoder(implicit config: IamClientConfig): GraphEncoder[Id, Resolver] =
    GraphEncoder {
      case (rootNode, r: InProjectResolver) => RootedGraph(rootNode, r.mainTriples(nxv.InProject))
      case (rootNode, r @ CrossProjectResolver(resTypes, projects, identities, _, _, _, _, _)) =>
        val triples = r.mainTriples(nxv.CrossProject) ++ r.triplesFor(identities) ++ r.triplesFor(resTypes)
        val graph   = Graph(triples).add(rootNode, nxv.projects, projects.map(_.show).map(literal))
        RootedGraph(rootNode, graph)
    }

  implicit def resolverGraphEncoderEither(implicit config: IamClientConfig): GraphEncoder[EncoderResult, Resolver] =
    resolverGraphEncoder.toEither

  private implicit class ResolverSyntax(resolver: Resolver) {
    private val s = IriNode(resolver.id)

    def mainTriples(tpe: AbsoluteIri): Set[Triple] =
      Set(
        (s, rdf.tpe, nxv.Resolver),
        (s, rdf.tpe, tpe),
        (s, nxv.priority, resolver.priority),
        (s, nxv.deprecated, resolver.deprecated),
        (s, nxv.rev, resolver.rev)
      )

    def triplesFor(identities: List[Identity])(implicit config: IamClientConfig): Set[Triple] =
      identities.foldLeft(Set.empty[Triple]) { (acc, identity) =>
        val (identityId, triples) = triplesFor(identity)
        acc + ((s, nxv.identities, identityId)) ++ triples
      }

    def triplesFor(resourceTypes: Set[AbsoluteIri]): Set[Triple] =
      resourceTypes.map(r => (s, nxv.resourceTypes, IriNode(r)): Triple)

    private def triplesFor(identity: Identity)(implicit config: IamClientConfig): (IriNode, Set[Triple]) = {
      val ss = IriNode(identity.id)
      identity match {
        case User(sub, realm)     => ss -> Set((ss, rdf.tpe, nxv.User), (ss, nxv.realm, realm), (ss, nxv.subject, sub))
        case Group(group, realm)  => ss -> Set((ss, rdf.tpe, nxv.Group), (ss, nxv.realm, realm), (ss, nxv.group, group))
        case Authenticated(realm) => ss -> Set((ss, rdf.tpe, nxv.Authenticated), (ss, nxv.realm, realm))
        case _                    => ss -> Set((ss, rdf.tpe, nxv.Anonymous))
      }
    }
  }
}
