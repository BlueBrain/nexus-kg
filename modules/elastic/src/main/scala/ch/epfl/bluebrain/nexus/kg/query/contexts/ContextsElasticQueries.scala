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

class ContextsElasticQueries[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[ContextId]])
    extends BaseElasticQueries[F, ContextId](elasticClient, settings) {

  implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)

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
  def apply[F[_]](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit
      rs: HttpClient[F, QueryResults[ContextId]]): ContextsElasticQueries[F] =
    new ContextsElasticQueries(elasticClient, settings)
}
