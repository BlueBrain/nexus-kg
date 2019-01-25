package ch.epfl.bluebrain.nexus.kg.search

import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.syntax._
import io.circe.{Encoder, Json}

object QueryBuilder {

  private def baseQuery(filterTerms: List[Json]): Json =
    Json.obj("sort" -> Json.arr(Json.fromString("_createdAt")),
             "query" -> Json.obj(
               "bool" -> Json.obj(
                 "filter" -> Json.arr(filterTerms: _*)
               )
             ))

  private def term[A: Encoder](k: String, value: A): Json =
    Json.obj("term" -> Json.obj(k -> value.asJson))

  /**
    * Build ElasticSearch search query from deprecation status and schema
    *
    * @param params the search parameters to perform the query
    * @return ElasticSearch query
    */
  def queryFor(params: SearchParams): Json =
    baseQuery(
      params.types.map(term("@type", _)) ++ params.id.map(term("@id", _)) ++ params.schema.map(
        term(nxv.constrainedBy.prefix, _)) ++ params.deprecated
        .map(term(nxv.deprecated.prefix, _)) ++ params.rev.map(term(nxv.rev.prefix, _)) ++ params.createdBy.map(
        term(nxv.createdBy.prefix, _)) ++ params.updatedBy.map(term(nxv.updatedBy.prefix, _)))
}
