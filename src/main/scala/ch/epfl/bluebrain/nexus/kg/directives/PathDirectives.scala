package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.PathMatcher1
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.{Curie, Iri}

object PathDirectives {

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]] by:
    *
    * Mapping the segment to an alias on the prefixMappings or
    * Converting the segment to an [[AbsoluteIri]] or
    * Converting the segment to a [[Curie]] and afterwards to an [[AbsoluteIri]] or
    * Joining the ''base'' with the segment and create an [[AbsoluteIri]] from it.
    *
    * @param project the project with its prefixMappings used to expand the alias or curie into an [[AbsoluteIri]]
    */
  def IdSegment(implicit project: Project): PathMatcher1[AbsoluteIri] = {
    def toAbsolute(curie: Curie): Either[String, AbsoluteIri] =
      project.prefixMappings
        .get(curie.prefix.show)
        .toRight(s"Unable to find prefix '${curie.prefix.show}' in the provided mapping")
        .map { p =>
          p + curie.reference.show
        }

    Segment flatMap { s =>
      project.prefixMappings.get(s) orElse
        Curie(s).flatMap(toAbsolute).toOption orElse
        Iri.url(s).toOption orElse
        Iri.absolute(project.base.asString + s).toOption
    }
  }
}
