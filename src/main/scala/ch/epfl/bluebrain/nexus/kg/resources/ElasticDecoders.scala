package ch.epfl.bluebrain.nexus.kg.resources

import akka.http.scaladsl.model.Uri.Path.{Segment, Slash}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.akka.all._
import io.circe.Decoder

object ElasticDecoders {

  /**
    * Circe decoder which which reconstructs resource representation ID from ElasticSearch response
    * @param project project to which the resource belongs
    * @param config  http config
    * @return        Decoder for representation ID of the resource
    */
  implicit def resourceIdDecoder(implicit project: Project, config: HttpConfig): Decoder[AbsoluteIri] =
    Decoder.decodeJsonObject.emap { json =>
      for {
        id <- json("@id").flatMap(_.asString).map(Iri.absolute).getOrElse(Left("Field: '@id' not found"))
        projectIri <- json("project")
          .flatMap(_.asString)
          .map(Iri.absolute)
          .getOrElse(Left("Field: 'project' not found"))
        schema <- json("constrainedBy")
          .flatMap(_.asString)
          .map(Iri.absolute)
          .getOrElse(Left("Field: 'constrainedBy' not found"))
        publicIri <- config.publicUri.toIri.asAbsolute match {
          case Some(absolute) => Right(absolute)
          case None           => Left("public uri needs to be absolute")
        }
        reprId = publicIri + "resources" + orgSegment(projectIri) + projectSegment(projectIri) + aliasOrCurieFor(
          schema,
          project) + aliasOrCurieFor(id, project)
      } yield reprId
    }

  private def orgSegment(projectIri: AbsoluteIri): String = projectIri.path.reverse match {
    case Segment(_, Slash(Segment(org, _))) => org
    case _                                  => throw new IllegalArgumentException(s"Invalid project url: ${projectIri.show}")
  }

  private def projectSegment(projectIri: AbsoluteIri): String = projectIri.path.reverse match {
    case Segment(proj, _) => proj
    case _                => throw new IllegalArgumentException(s"Invalid project url: ${projectIri.show}")
  }
  private def aliasOrCurieFor(iri: AbsoluteIri, project: Project): String = {
    def prefixMatches(iri: Iri.AbsoluteIri, prefix: Iri.AbsoluteIri): Boolean =
      iri.show.startsWith(prefix.show) || iri == prefix

    project.prefixMappings.find { case (_, iri2) => prefixMatches(iri, iri2) } match {
      case Some((prefix, matchedIri)) if matchedIri == iri => prefix
      case Some((prefix, matchedIri)) if iri.show.startsWith(matchedIri.show) =>
        s"$prefix:${iri.show.stripPrefix(matchedIri.show)}"
      case None => iri.show
    }
  }

}
