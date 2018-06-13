package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.PathMatcher1
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project.LoosePrefixMapping
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

object PathDirectives {

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]] from ''mappings''
    *
    * @param mappings the prefixMappings used to generate the [[AbsoluteIri]]
    */
  def aliasOrCurie(implicit mappings: List[LoosePrefixMapping]): PathMatcher1[AbsoluteIri] = {
    Segment flatMap { s =>
      val (prefix, ref) = s.split(":", 2) match {
        case Array(p, r) if p.nonEmpty => (p, r)
        case _                         => (s, "")
      }
      prefixMappings.get(prefix).flatMap(p => Iri.absolute(s"${p.show}$ref").toOption)
    }
  }

  private def prefixMappings(implicit mappings: List[LoosePrefixMapping]): Map[String, AbsoluteIri] =
    mappings.foldLeft(Map.empty[String, AbsoluteIri]) {
      case (acc, LoosePrefixMapping(prefix, ns)) =>
        acc + (prefix.value -> Iri.absolute(ns.value).toOption.get)
    }
}
