package ch.epfl.bluebrain.nexus.kg.service.io

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys._
import io.circe._

class BaseEncoder(prefixes: PrefixUris) {

  /**
    * Syntax to extend JSON objects in response body with the various contexts used internally.
    *
    * @param json the JSON object
    */
  implicit class JsonOps(json: Json) {

    /**
      * Adds or merges the core context URI to an existing JSON object.
      */
    def addCoreContext: Json = addContext(prefixes.CoreContext)

    /**
      * Adds or merges the links context URI to an existing JSON object.
      */
    def addLinksContext: Json = addContext(prefixes.LinksContext)

    /**
      * Adds or merges the search context URI to an existing JSON object.
      */
    def addSearchContext: Json = addContext(prefixes.SearchContext)

    /**
      * Adds or merges the distribution (i.e. attachment) context URI to an existing JSON object.
      */
    def addDistributionContext: Json = addContext(prefixes.DistributionContext)

    /**
      * Adds or merges a context URI to an existing JSON object.
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
                case (Some(vo), _, _) if !vo.values.contains(contextUriString) =>
                  jo.add(`@context`, Json.arr(value, contextUriString))
                case (_, Some(va), _) if !va.contains(contextUriString) =>
                  jo.add(`@context`, Json.fromValues(va :+ contextUriString))
                case (_, _, Some(vs)) if vs != context.toString =>
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
