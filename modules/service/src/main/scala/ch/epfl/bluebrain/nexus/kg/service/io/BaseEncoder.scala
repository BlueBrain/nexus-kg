package ch.epfl.bluebrain.nexus.kg.service.io

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.BaseEncoder.JsonLDKeys._
import io.circe._
import io.circe.syntax._

class BaseEncoder(prefixes: PrefixUris) {

  implicit val linksEncoder: Encoder[Links] =
    Encoder.encodeJson.contramap { links =>
      links.values.mapValues {
        case href :: Nil => Json.fromString(s"$href")
        case hrefs       => Json.arr(hrefs.map(href => Json.fromString(s"$href")): _*)
      }.asJson
    }

  /**
    * Syntax to extend JSON objects in response body with the various contexts used internally.
    *
    * @param json the JSON object
    */
  implicit class JsonOps(json: Json) {

    def addCoreContext: Json = addContext(prefixes.CoreContext)

    def addLinksContext: Json = addContext(prefixes.LinksContext)

    def addSearchContext: Json = addContext(prefixes.SearchContext)

    /**
      * Adds or merges the standard context URI to an existing JSON object.
      *
      * @param context the standard context URI
      * @return a new JSON object
      */
    private def addContext(context: Uri): Json = {
      val contextUriString = Json.fromString(context.toString)

      json.asObject match {
        case Some(jo) =>
          val updated = jo(`@context`) match {
            case None => jo.add(`@context`, contextUriString)
            case Some(value) =>
              (value.asObject, value.asArray, value.asString) match {
                case (Some(_), _, _) =>
                  jo.add(`@context`, Json.arr(value, contextUriString))
                case (_, Some(va), _) =>
                  jo.add(`@context`, Json.fromValues(va :+ contextUriString))
                case (_, _, Some(_)) =>
                  jo.add(`@context`, Json.arr(value, contextUriString))
                case _ => jo
              }
          }
          Json.fromJsonObject(updated)
        case None => json
      }
    }
  }
}

object BaseEncoder {

  object JsonLDKeys {
    val `@context`     = "@context"
    val `@id`          = "@id"
    val links          = "links"
    val resultId       = "resultId"
    val source         = "source"
    val score          = "score"
    val nxvNs          = "nxv"
    val nxvRev         = "nxv:rev"
    val nxvDeprecated  = "nxv:deprecated"
    val nxvDescription = "nxv:description"
    val nxvPublished   = "nxv:published"
  }

}
