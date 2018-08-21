package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.server.PathMatcher.{Matched, Unmatched}
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.{PathMatcher, PathMatcher0, PathMatcher1}
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
  @SuppressWarnings(Array("MethodNames"))
  def IdSegment(implicit project: Project): PathMatcher1[AbsoluteIri] =
    Segment flatMap (toIri)

  private def toIri(s: String)(implicit project: Project): Option[AbsoluteIri] =
    project.prefixMappings.get(s) orElse
      Curie(s).flatMap(_.toIriUnsafePrefix(project.prefixMappings)).toOption orElse
      Iri.url(s).toOption orElse
      Iri.absolute(project.base.asString + s).toOption

  /**
    * Attempts to match a segment and build an [[AbsoluteIri]], as in the method ''IdSegment''.
    * It then attempts to match the resulting absolute iri to the provided ''iri''
    *
    * @param iri     the iri to match against the segment
    * @param project the project with its prefixMappings used to expand the alias or curie into an [[AbsoluteIri]]
    */
  def isIdSegment(iri: AbsoluteIri)(implicit project: Project): PathMatcher0 =
    new PathMatcher[Unit] {
      def apply(path: Path) = path match {
        case Path.Segment(segment, tail) =>
          toIri(segment) match {
            case Some(`iri`) => Matched(tail, ())
            case _           => Unmatched
          }
        case _ => Unmatched
      }
    }
}
