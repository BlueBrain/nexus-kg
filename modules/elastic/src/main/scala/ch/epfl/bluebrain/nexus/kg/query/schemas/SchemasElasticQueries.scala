package ch.epfl.bluebrain.nexus.kg.query.schemas

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.FullAccessControlList
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIds, ElasticIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries

/**
  * Elastic Search queries for schemas
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  */
class SchemasElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[SchemaId]],
    F: MonadError[F, Throwable])
    extends BaseElasticQueries[F, SchemaId](elasticClient, settings) {

  protected override val rdfType = "Schema".qualifyAsString

  /**
    * List all schemas within a schema name
    * @param pagination   pagination object
    * @param schemaName   schema name
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    *
    */
  def list(pagination: Pagination,
           schemaName: SchemaName,
           deprecated: Option[Boolean],
           published: Option[Boolean],
           acls: FullAccessControlList): F[QueryResults[SchemaId]] = {
    elasticClient.search[SchemaId](query(acls, termsFrom(deprecated, published) :+ schemaGroupTerm(schemaName): _*),
                                   Set(index))(pagination, sort = defaultSort)
  }

  /**
    * Index used for searching
    */
  override protected val index: String = ElasticIds.schemasIndex(prefix)
}

object SchemasElasticQueries {

  /**
    * Constructs new `SchemasElasticQueries` instance
    * @param elasticClient  Elastic Search client
    * @param settings       Elastic Search settings
    * @param rs             HTTP client
    * @tparam F             the monadic effect type
    * @return new `SchemasElasticQueries` instance
    */
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[SchemaId]],
      F: MonadError[F, Throwable]): SchemasElasticQueries[F] = {
    new SchemasElasticQueries(elasticClient, settings)
  }
}
