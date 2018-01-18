package ch.epfl.bluebrain.nexus.kg.core.ld

import java.time.Instant

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixUri.dateTime
import io.circe.Json
object JsonLdOps {

  /**
    * Interface syntax to expose new functionality into Uri type
    *
    * @param value the [[Uri]]
    */
  implicit class UriJsonLDSupportOps(value: Uri) {

    /**
      * Method exposed on Uri instances.
      *
      * @return Json which contains the key @id and the value uri ('{"@id": "uri"}')
      */
    def jsonLd: Json = Json.obj(idKey -> Json.fromString(value.toString()))
  }

  /**
    * Interface syntax to expose new functionality into Instant type
    *
    * @param value the [[Instant]]
    */
  implicit class InstantJsonLDSupportOps(value: Instant) {

    /**
      * Method exposed on Instant instances.
      *
      * @return Json representing a dateTime ('{"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value":"2015-01-25T12:34:56Z"}')
      */
    def jsonLd: Json = Json.obj(typeKey -> Json.fromString(s"$dateTime"), valueKey -> Json.fromString(value.toString))
  }
}
