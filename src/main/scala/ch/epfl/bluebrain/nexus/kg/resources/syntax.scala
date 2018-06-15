package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import cats.Functor
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{nxv, xsd}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.Literal
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import com.github.ghik.silencer.silent
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._

object syntax {

  final implicit class RefSyntax[F[_]](ref: Ref)(implicit R: Resolution[F]) {
    private val schacl: AbsoluteIri   = nxv.ShaclSchema
    private val ontology: AbsoluteIri = nxv.OntologySchema

    def resolve: F[Option[Resource]] = R.resolve(ref)

    def resolveOr(f: Ref => Rejection)(implicit F: Functor[F]): EitherT[F, Rejection, Resource] =
      EitherT.fromOptionF(resolve, f(ref))

    def resolveAll: F[List[Resource]] = R.resolveAll(ref)

    /**
      * @return the types inferred by the [[Ref]]
      */
    def types: Set[AbsoluteIri] =
      ref.iri match {
        case `schacl`   => Set(nxv.Schema)
        case `ontology` => Set(nxv.Ontology)
        case _          => Set.empty
      }

  }

  final implicit class ResourceSyntax(resource: ResourceF[_, _, _]) {
    def isSchema: Boolean = resource.types.contains(nxv.Schema.value)
  }

  final implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)

  @SuppressWarnings(Array("UnusedMethodParameter"))
  final implicit def toNode(@silent identity: Identity): Node =
    Literal(nxv.Anonymous.value.asUri)

  final implicit class WithReplace(g: Graph) {
    def replaceNode(target: Node.IriOrBNode, replacement: Node.IriOrBNode): Graph = {
      val triples = g.select(s = _ == target) ++ g.select(o = _ == target)
      Graph(triples.map {
        case (s, p, o) =>
          val ns = if (s == target) replacement else s
          val no = if (o == target) replacement else o
          (ns, p, no)
      })
    }
  }
}
