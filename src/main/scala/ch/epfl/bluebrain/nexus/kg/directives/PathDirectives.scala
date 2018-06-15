package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.PathMatcher1
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

object PathDirectives {

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]] from the project prefixMappings
    *
    * @param project the project with its prefixMappings used to generate the [[AbsoluteIri]]
    */
  def aliasOrCurie(implicit project: Project): PathMatcher1[AbsoluteIri] = {
    Segment flatMap { s =>
      val (prefix, ref) = s.split(":", 2) match {
        case Array(p, r) if p.nonEmpty => (p, r)
        case _                         => (s, "")
      }
      project.prefixMappings.get(prefix).flatMap(p => Iri.absolute(s"${p.show}$ref").toOption)
    }
  }
}
