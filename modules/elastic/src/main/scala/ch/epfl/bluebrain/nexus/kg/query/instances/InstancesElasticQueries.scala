package ch.epfl.bluebrain.nexus.kg.query.instances

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries
import io.circe.Json

class InstancesElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[InstanceId]])
    extends BaseElasticQueries[F, InstanceId](elasticClient, settings) {

  private def schemaTerm(schemaId: SchemaId): Json = term("schema".qualifyAsString, schemaId.qualifyAsString)

  def list(pagination: Pagination,
           schemaName: SchemaName,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[InstanceId]] = {
    elasticClient.search[InstanceId](query(termsFrom(deprecated, published) :+ schemaGroupTerm(schemaName): _*))(
      pagination,
      sort = defaultSort)
  }

  def list(pagination: Pagination,
           schemaId: SchemaId,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[InstanceId]] = {
    elasticClient.search[InstanceId](query(termsFrom(deprecated, published) :+ schemaTerm(schemaId): _*))(pagination,
                                                                                                          sort =
                                                                                                            defaultSort)
  }

  override protected val rdfType: String = "Instance".qualifyAsString
}

object InstancesElasticQueries {
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[InstanceId]]): InstancesElasticQueries[F] =
    new InstancesElasticQueries(elasticClient, settings)
}
