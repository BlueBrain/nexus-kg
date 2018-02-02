package ch.epfl.bluebrain.nexus.kg.query.domains

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries

class DomainsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[DomainId]])
    extends BaseElasticQueries[F, DomainId](elasticClient, settings) {
  override protected val rdfType: String = "Domain".qualifyAsString
}

object DomainsElasticQueries {
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[DomainId]]): DomainsElasticQueries[F] =
    new DomainsElasticQueries(elasticClient, settings)
}
