package ch.epfl.bluebrain.nexus.kg.resources

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Decoder

object ElasticDecoders {

  /**
    * Circe decoder which reconstructs resource representation ID from ElasticSearch response
    *
    * @param prefix  prefix to use for representation IDs
    * @param project project to which the resource belongs
    * @return        Decoder for representation ID of the resource
    */
  implicit def resourceIdDecoder(prefix: AbsoluteIri)(implicit project: Project): Decoder[AbsoluteIri] =
    Decoder.decodeJsonObject.emap { json =>
      for {
        id <- json("@id").flatMap(_.asString).map(Iri.absolute).getOrElse(Left("Field: '@id' not found"))
        schema <- json("constrainedBy")
          .flatMap(_.asString)
          .map(Iri.absolute)
          .getOrElse(Left("Field: 'constrainedBy' not found"))
        reprId = prefix + aliasOrCurieFor(schema, project) + aliasOrCurieFor(id, project)
      } yield reprId
    }

  private def aliasOrCurieFor(iri: AbsoluteIri, project: Project): String = {
    project.prefixMappings
      .collectFirst {
        case (prefix, ns) if iri.show.startsWith(ns.show) =>
          s"$prefix:${iri.show.stripPrefix(ns.show)}"
        case (prefix, ns) if iri == ns =>
          prefix
      }
      .getOrElse(iri.show)
  }

}
