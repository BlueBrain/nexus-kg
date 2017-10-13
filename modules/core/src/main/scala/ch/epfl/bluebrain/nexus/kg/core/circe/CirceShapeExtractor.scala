package ch.epfl.bluebrain.nexus.kg.core.circe

import io.circe.Json

import scala.util.matching.Regex

/**
  * Typeclass definitions that describes extracting a shape identified by A from a Json structure.
  *
  * @tparam A generic type parameter for the shape id
  */
trait CirceShapeExtractor[A] {

  /**
    * Extract the targeted shape from a Json payload.
    *
    * @param id    the id value where to filter the shape from.
    * @param value the json payload from where to extract the shape
    * @return an optional Json which contains only the filtered shape.
    */
  def extract(id: A, value: Json): Option[Json]
}

object CirceShapeExtractorInstances extends CirceShapeExtractorSyntax {

  /**
    * Shape extractor implementation using optics and regex.
    * Matches the shape id based on a regex.
    */
  implicit val regexStringExtractor = new CirceShapeExtractor[String] {
    override def extract(id: String, value: Json): Option[Json] = {
      val escapedId = Regex.quote(id)
      import io.circe.optics.JsonPath._
      val r = s"""^*.(/|:)$escapedId$$""".r

      root.shapes.each
        .filter(
          _.hcursor
            .get[String]("@id")
            .fold(_ => false, r.findFirstIn(_).isDefined))
        .json
        .headOption(value)
    }
  }

}

trait CirceShapeExtractorSyntax {

  /**
    * Interface syntax to expose new functionality into Json type
    *
    * @param value json payload
    * @tparam A generic type parameter for the shape id
    */
  implicit class CirceSchemaExtractorOps[A](value: Json) {

    /**
      * Method exposed on Json instances
      *
      * @param id        the id value where to filter the shape from.
      * @param extractor instances of the type class as implicit parameter
      * @return an optional Json which contains only the filtered shape.
      */
    def fetchShape(id: A)(implicit extractor: CirceShapeExtractor[A]): Option[Json] =
      extractor.extract(id, value)
  }

}
