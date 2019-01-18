package ch.epfl.bluebrain.nexus.kg.resolve

import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import io.circe.Encoder

/**
  * Encoders for [[Resolver]]
  */
object ResolverEncoder {

  implicit def qrResolverEncoder: Encoder[QueryResults[Resolver]] =
    qrsEncoder[Resolver](resolverCtx mergeContext resourceCtx) mapJson (_ addContext resolverCtxUri)

  implicit val resolverGraphEncoder: GraphEncoder[Resolver] = GraphEncoder {
    case r: InProjectResolver => IriNode(r.id) -> Graph(r.mainTriples(nxv.InProject))
    case r @ CrossProjectResolver(resourceTypes, _, identities, _, _, _, _, _) =>
      val s: IriOrBNode            = IriNode(r.id)
      val projTriples: Set[Triple] = r.projectsString.map(p => (s, nxv.projects, p): Triple)
      s -> Graph(
        r.mainTriples(nxv.CrossProject) ++ r.triplesFor(identities) ++ r.triplesFor(resourceTypes) ++ projTriples)
  }

  private implicit def qqResolverEncoder(implicit enc: GraphEncoder[Resolver]): GraphEncoder[QueryResult[Resolver]] =
    GraphEncoder { res =>
      val encoded = enc(res.source)
      encoded.subject -> encoded.graph
    }

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

    def triplesFor(identities: List[Identity]): Set[Triple] =
      identities.foldLeft(Set.empty[Triple]) { (acc, identity) =>
        val (bNode, triples) = triplesFor(identity)
        acc + ((s, nxv.identities, bNode)) ++ triples
      }

    def triplesFor(resourceTypes: Set[AbsoluteIri]): Set[Triple] =
      resourceTypes.map(r => (s, nxv.resourceTypes, IriNode(r)): Triple)

    private def triplesFor(identity: Identity): (BNode, Set[Triple]) = {
      val ss = blank
      identity match {
        case User(sub, realm)     => ss -> Set((ss, rdf.tpe, nxv.User), (ss, nxv.realm, realm), (ss, nxv.subject, sub))
        case Group(group, realm)  => ss -> Set((ss, rdf.tpe, nxv.Group), (ss, nxv.realm, realm), (ss, nxv.group, group))
        case Authenticated(realm) => ss -> Set((ss, rdf.tpe, nxv.Authenticated), (ss, nxv.realm, realm))
        case _                    => ss -> Set((ss, rdf.tpe, nxv.Anonymous))
      }
    }
  }
}
