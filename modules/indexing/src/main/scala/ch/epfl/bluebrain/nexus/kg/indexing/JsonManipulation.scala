package ch.epfl.bluebrain.nexus.kg.indexing

import io.circe.{Json, JsonObject}
import io.circe.syntax._

object JsonManipulation {

  /**
    * Interface syntax to expose new functionality into Json type
    *
    * @param json json payload
    */
  implicit class JsonManipulationSyntax(json: Json) {

    /**
      * Method exposed on Json instances.
      *
      * @param keys list of ''keys'' to be removed from the top level of the ''json''
      * @return the original json without the provided ''keys'' on the top level of the structure
      */
    def removeKeys(keys: String*): Json = {
      def removeKeys(obj: JsonObject): Json =
        keys.foldLeft(obj)((accObj, key) => accObj.remove(key)).asJson

      json.arrayOrObject[Json](json, arr => arr.map(j => j.removeKeys(keys: _*)).asJson, obj => removeKeys(obj))
    }
  }

}
