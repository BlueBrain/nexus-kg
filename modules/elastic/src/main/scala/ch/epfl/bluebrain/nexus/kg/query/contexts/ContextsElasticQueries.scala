package ch.epfl.bluebrain.nexus.kg.query.contexts

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries
import io.circe.Json

/**
  * Elastic Search queries for contexts
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  */
class ContextsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[ContextId]])
    extends BaseElasticQueries[F, ContextId](elasticClient, settings) {

  implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)

  /**
    * List all contexts within a context name
    * @param pagination   pagination object
    * @param contextName  context name
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @return query results
    *
    */
  def list(pagination: Pagination,
           contextName: ContextName,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[ContextId]] = {
    elasticClient.search[ContextId](query(termsFrom(deprecated, published) :+ contextGroupTerm(contextName): _*))(
      pagination,
      sort = defaultSort)
  }

  override protected val rdfType: String = "Context".qualifyAsString
  private def contextGroupTerm(contextName: ContextName): Json =
    term("contextGroup".qualifyAsString, contextName.qualifyAsString)

}

object ContextsElasticQueries {

  /**
    * Constructs new `ContextsElasticQueries` instance
    * @param elasticClient  Elastic Search client
    * @param settings       Elastic Search settings
    * @param rs             HTTP client
    * @tparam F             the monadic effect type
    * @return new `ContextsElasticQueries` instance
    */
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[ContextId]]): ContextsElasticQueries[F] =
    new ContextsElasticQueries(elasticClient, settings)
}
