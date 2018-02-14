package ch.epfl.bluebrain.nexus.kg.query.instances

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.FullAccessControlList
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIds, ElasticIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries
import io.circe.Json

/**
  * Elastic Search queries for instances
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  */
class InstancesElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[InstanceId]],
    F: MonadError[F, Throwable])
    extends BaseElasticQueries[F, InstanceId](elasticClient, settings) {

  private def schemaTerm(schemaId: SchemaId): Json = term("schema".qualifyAsString, schemaId.qualifyAsString)

  /**
    * List all objects of a given type within a domain
    * @param pagination   pagination object
    * @param domainId     domain ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    *
    */
  override def list(pagination: Pagination,
                    domainId: DomainId,
                    deprecated: Option[Boolean],
                    published: Option[Boolean],
                    acls: FullAccessControlList): F[QueryResults[InstanceId]] = {
    if (hasReadPermissionsFor(domainId, acls)) {
      elasticClient.search[InstanceId](
        query(acls, termsFrom(deprecated, published) :+ domainTerm(domainId): _*),
        Set(ElasticIds.domainInstancesIndex(prefix, domainId)))(pagination, sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[InstanceId]]))
    }
  }

  /**
    * List all instances within a schema name
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
           acls: FullAccessControlList): F[QueryResults[InstanceId]] = {
    if (hasReadPermissionsFor(schemaName.domainId, acls)) {
      elasticClient.search[InstanceId](
        query(acls, termsFrom(deprecated, published) :+ schemaGroupTerm(schemaName): _*),
        Set(ElasticIds.domainInstancesIndex(prefix, schemaName.domainId)))(pagination, sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[InstanceId]]))
    }
  }

  /**
    * List all schemas within a schema ID
    * @param pagination   pagination object
    * @param schemaId     schema ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    *
    */
  def list(pagination: Pagination,
           schemaId: SchemaId,
           deprecated: Option[Boolean],
           published: Option[Boolean],
           acls: FullAccessControlList): F[QueryResults[InstanceId]] = {
    if (hasReadPermissionsFor(schemaId.domainId, acls)) {
      elasticClient.search[InstanceId](
        query(acls, termsFrom(deprecated, published) :+ schemaTerm(schemaId): _*),
        Set(ElasticIds.domainInstancesIndex(prefix, schemaId.domainId)))(pagination, sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[InstanceId]]))
    }
  }

  override protected val rdfType: String = "Instance".qualifyAsString

  /**
    * Default index used for searching
    */
  override protected val index: String = s"${ElasticIds.instancesIndexPrefix(prefix)}_*"
}

object InstancesElasticQueries {

  /**
    * Constructs new `InstancesElasticQueries` instance
    * @param elasticClient  Elastic Search client
    * @param settings       Elastic Search settings
    * @param rs             HTTP client
    * @tparam F             the monadic effect type
    * @return new `InstancesElasticQueries` instance
    */
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[InstanceId]],
      F: MonadError[F, Throwable]): InstancesElasticQueries[F] =
    new InstancesElasticQueries(elasticClient, settings)
}
