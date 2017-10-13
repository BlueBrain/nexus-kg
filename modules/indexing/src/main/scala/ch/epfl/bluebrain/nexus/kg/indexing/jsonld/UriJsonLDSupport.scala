package ch.epfl.bluebrain.nexus.kg.indexing.jsonld

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import io.circe.Json

object UriJsonLDSupport {

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
}
