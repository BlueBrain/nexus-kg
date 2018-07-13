package ch.epfl.bluebrain.nexus.kg.search

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json

object QueryBuilder {

  private def baseQuery(filterTerms: List[Json]): Json =
    Json.obj(
      "query" -> Json.obj(
        "bool" -> Json.obj(
          "filter" -> Json.arr(filterTerms: _*)
        )
      ))

  private def deprecatedTerm(deprecatedOpt: Option[Boolean]): List[Json] = deprecatedOpt match {
    case Some(deprecated) =>
      List(
        Json.obj(
          "term" -> Json.obj(
            "_deprecated" -> Json.fromBoolean(deprecated)
          )
        ))
    case None => List.empty
  }

  /**
    * Build Elastic search query from deprecation status and schema
    * @param deprecated optional deprecation status
    * @param schema     schema to filter resources by
    * @return           ElasticSearch query
    */
  def queryFor(deprecated: Option[Boolean], schema: AbsoluteIri): Json = baseQuery(
    Json.obj(
      "term" -> Json.obj(
        "_constrainedBy" -> Json.fromString(schema.show)
      )
    ) :: deprecatedTerm(deprecated)
  )

  /**
    * Build Elastic search query from deprecation status
    * @param deprecated optional deprecation status
    * @return           ElasticSearch query
    */
  def queryFor(deprecated: Option[Boolean]): Json = baseQuery(deprecatedTerm(deprecated))

}
