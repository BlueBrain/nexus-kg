package ch.epfl.bluebrain.nexus.kg.query.organizations

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries

class OrganizationsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[OrgId]])
    extends BaseElasticQueries[F, OrgId](elasticClient, settings) {
  override protected val rdfType: String = "Organization".qualifyAsString
}

object OrganizationsElasticQueries {
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[OrgId]]): OrganizationsElasticQueries[F] =
    new OrganizationsElasticQueries(elasticClient, settings)
}
