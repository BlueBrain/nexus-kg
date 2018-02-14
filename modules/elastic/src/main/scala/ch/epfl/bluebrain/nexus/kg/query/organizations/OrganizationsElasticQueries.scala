package ch.epfl.bluebrain.nexus.kg.query.organizations

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.query.BaseElasticQueries
import io.circe.Json

/**
  * Elastic Search queries for Organizations
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  */
class OrganizationsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[OrgId]],
    F: MonadError[F, Throwable])
    extends BaseElasticQueries[F, OrgId](elasticClient, settings) {
  override protected val rdfType: String = "Organization".qualifyAsString

  override protected def orgTerm(orgId: OrgId): Json = term("@id", orgId.qualifyAsString)
}

object OrganizationsElasticQueries {

  /**
    * Constructs new `OrganizationsElasticQueries` instance
    * @param elasticClient  Elastic Search client
    * @param settings       Elastic Search settings
    * @param rs             HTTP client
    * @tparam F             the monadic effect type
    * @return new `OrganizationsElasticQueries` instance
    */
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[OrgId]],
      F: MonadError[F, Throwable]): OrganizationsElasticQueries[F] =
    new OrganizationsElasticQueries(elasticClient, settings)
}
