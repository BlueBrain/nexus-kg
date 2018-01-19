package ch.epfl.bluebrain.nexus.kg.service.io

import ch.epfl.bluebrain.nexus.commons.http.JsonOps.JsonOpsSyntax
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import io.circe._

class BaseEncoder(prefixes: PrefixUris) {

  /**
    * Syntax to extend JSON objects in response body with the various contexts used internally.
    *
    * @param json the JSON object
    */
  implicit class JsonOpsWithContextSyntax(json: Json) extends JsonOpsSyntax(json) {

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

  }

}
