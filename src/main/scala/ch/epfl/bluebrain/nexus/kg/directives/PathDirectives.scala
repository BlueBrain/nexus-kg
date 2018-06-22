package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.http.scaladsl.server.{Directive1, PathMatcher1}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

object PathDirectives {

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]] by expanding the value using the project prefixMappings.
    *
    * @param project the project with its prefixMappings used to expand the alias or curie into an [[AbsoluteIri]]
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

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]] by expanding the value using the project prefixMappings.
    * Consumes the path segment if matches.
    *
    * @param project the project with its prefixMappings used to expand the alias or curie into an [[AbsoluteIri]]
    */
  def aliasOrCuriePrefix(implicit project: Project): Directive1[AbsoluteIri] =
    pathPrefix(aliasOrCurie)

  /**
    * Attempts to match a last path segment and build an [[AbsoluteIri]] by expanding the value using the project
    * prefixMappings. It matches if the path ends or ends with a single slash.
    *
    * @param project the project with its prefixMappings used to expand the alias or curie into an [[AbsoluteIri]]
    */
  def aliasOrCuriePath(implicit project: Project): Directive1[AbsoluteIri] =
    aliasOrCuriePrefix & pathEndOrSingleSlash
}
