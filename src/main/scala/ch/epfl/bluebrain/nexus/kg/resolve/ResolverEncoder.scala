package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
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

/**
  * Encoders for [[Resolver]]
  */
object ResolverEncoder {

  implicit val resolverRootNode: RootNode[Resolver] = r => IriNode(r.id)

  def json(qrsResolvers: QueryResults[Resolver])(implicit enc: GraphEncoder[EncoderResult, QueryResults[Resolver]],
                                                 node: RootNode[QueryResults[Resolver]]): DecoderResult[Json] =
    QueryResultEncoder.json(qrsResolvers, resolverCtx mergeContext resourceCtx).map(_ addContext resolverCtxUri)

  implicit val resolverGraphEncoder: GraphEncoder[Id, Resolver] = GraphEncoder {
    case (rootNode, r: InProjectResolver) => RootedGraph(rootNode, r.mainTriples(nxv.InProject))
    case (rootNode, r @ CrossProjectResolver(resTypes, _, identities, _, _, _, _, _)) =>
      val projectsString = r match {
        case CrossProjectResolver(_, `Set[ProjectRef]`(projects), _, _, _, _, _, _)   => projects.map(_.show)
        case CrossProjectResolver(_, `Set[ProjectLabel]`(projects), _, _, _, _, _, _) => projects.map(_.show)
      }
      val projTriples: Set[Triple] = projectsString.map(p => (rootNode, nxv.projects, p): Triple)
      RootedGraph(rootNode,
                  r.mainTriples(nxv.CrossProject) ++ r.triplesFor(identities) ++ r.triplesFor(resTypes) ++ projTriples)
  }

  implicit val resolverGraphEncoderEither: GraphEncoder[EncoderResult, Resolver] = resolverGraphEncoder.toEither

  implicit def qqResolverEncoder: GraphEncoder[Id, QueryResult[Resolver]] =
    GraphEncoder { (rootNode, res) =>
      resolverGraphEncoder(rootNode, res.source)
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
