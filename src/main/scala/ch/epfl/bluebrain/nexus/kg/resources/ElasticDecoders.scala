package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Decoder

object ElasticDecoders {

  /**
    * Circe decoder which reconstructs resource representation ID from ElasticSearch response
    *
    * @param labeledProject project to which the resource belongs
    * @return        Decoder for representation ID of the resource
    */
  implicit def resourceIdDecoder(implicit labeledProject: LabeledProject, http: HttpConfig): Decoder[AbsoluteIri] =
    Decoder.decodeJsonObject.emap { json =>
      for {
        id <- json("@id").flatMap(_.asString).map(Iri.absolute).getOrElse(Left("Field: '@id' not found"))
        schema <- json("_constrainedBy")
          .flatMap(_.asString)
          .map(Iri.absolute)
          .getOrElse(Left("Field: '_constrainedBy' not found"))
      } yield AccessId(id, schema)
    }
}
