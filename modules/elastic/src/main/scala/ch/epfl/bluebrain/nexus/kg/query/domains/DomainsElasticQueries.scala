package ch.epfl.bluebrain.nexus.kg.query.domains

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries

/**
  * Elastic Search queries for domains
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  */
class DomainsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[DomainId]],
    F: MonadError[F, Throwable])
    extends BaseElasticQueries[F, DomainId](elasticClient, settings) {
  override protected val rdfType: String = "Domain".qualifyAsString
}

object DomainsElasticQueries {

  /**
    * Constructs new `DomainsElasticQueries` instance
    * @param elasticClient  Elastic Search client
    * @param settings       Elastic Search settings
    * @param rs             HTTP client
    * @tparam F             the monadic effect type
    * @return new `DomainsElasticQueries` instance
    */
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[DomainId]],
      F: MonadError[F, Throwable]): DomainsElasticQueries[F] =
    new DomainsElasticQueries(elasticClient, settings)
}
