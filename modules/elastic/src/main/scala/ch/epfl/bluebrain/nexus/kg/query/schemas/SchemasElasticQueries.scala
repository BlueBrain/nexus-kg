package ch.epfl.bluebrain.nexus.kg.query.schemas

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries

class SchemasElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[SchemaId]])
    extends BaseElasticQueries[F, SchemaId](elasticClient, settings) {

  protected override val rdfType = "Schema".qualifyAsString

  def list(pagination: Pagination,
           schemaName: SchemaName,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[SchemaId]] = {
    elasticClient.search[SchemaId](query(termsFrom(deprecated, published) :+ schemaGroupTerm(schemaName): _*))(
      pagination,
      sort = defaultSort)
  }

}

object SchemasElasticQueries {
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[SchemaId]]): SchemasElasticQueries[F] = {
    new SchemasElasticQueries(elasticClient, settings)
  }
}
