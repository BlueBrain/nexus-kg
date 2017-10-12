package ch.epfl.bluebrain.nexus.kg.indexing.jsonld

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import io.circe.Json

/**
  * Typeclass definition that adds JSON-LD support into [[Uri]]
  *
  */
trait UriJsonLDSupport {

  /**
    * Convert a Uri into a JSON-LD @id property
    *
    * @param value the [[Uri]]
    */
  def toId(value: Uri): Json
}

object UriJsonLDSupport {

  implicit val uriJsonLDSupportInstance = new UriJsonLDSupport {
    override def toId(value: Uri): Json =
      Json.obj(idKey -> Json.fromString(value.toString()))
  }

  /**
    * Interface syntax to expose new functionality into Uri type
    *
    * @param value the [[Uri]]
    */
  implicit class UriJsonLDSupportOps(value: Uri) {
    /**
      * Method exposed on Uri instances
      *
      * @param jsonLdSupport implicitly available instance of the type class
      * @return Json which contains the key @id and the value uri ('{"@id": "uri"}')
      */
    def jsonLd(implicit jsonLdSupport: UriJsonLDSupport): Json =
      jsonLdSupport.toId(value)
  }

}