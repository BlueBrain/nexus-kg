package ch.epfl.bluebrain.nexus.kg.indexing

import io.circe.Json

/**
  * A builder of queries for patch ElasticSearch operations.
  */
object PatchQuery {

  /**
    * Builds a ElasticSearch update script which adds the fields in ''json''
    * while removing the provided ''removeKeys'' to a document.
    *
    * @param json       the json to be added in the update query
    * @param removeKeys the keys to be removed from the existing document
    */
  def apply(json: Json, removeKeys: String*): Json = {
    val script = removeKeys
      .foldLeft(StringBuilder.newBuilder)((acc, field) => acc.append(s"""ctx._source.remove("$field");"""))
    Json.obj(
      "script" -> Json.obj("source" -> Json.fromString(s"${script}ctx._source.putAll(params.value)"),
                           "params" -> Json.obj("value" -> json)))

  }

  /**
    * Builds a ElasticSearch update script which overrides the existing document with the fields in ''json''
    * while keeping the fields from the original document provided in ''keepKeys''.
    *
    * @param json     the json to be added in the update query
    * @param keepKeys the keys to be kept from the existing document
    */
  def inverse(json: Json, keepKeys: String*): Json = {
    val (prefix, sufix) = keepKeys.zipWithIndex.foldLeft(StringBuilder.newBuilder -> StringBuilder.newBuilder) {
      case ((p, s), (key, index)) =>
        val variable = s"var$index"
        (p.append(s"""Object $variable = ctx._source.get("$key");""")) -> (s.append(s"""if($variable != null) ctx._source.put("$key", $variable);"""))
    }
    val script = s"""$prefix ctx._source = params.value; $sufix"""
    Json.obj("script" -> Json.obj("source" -> Json.fromString(script), "params" -> Json.obj("value" -> json)))
  }

}
