package ch.epfl.bluebrain.nexus.kg.query

import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults, Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping.rdfTypeKey
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import io.circe.Json

abstract class BaseElasticQueries[F[_], Id](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[Id]]) {

  val ElasticIndexingSettings(_, _, base, baseVoc) = settings
  protected val defaultSort                        = SortList(List(Sort(Sort.OrderType.Asc, "@id")))

  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](base)
  implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
  implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](baseVoc)
  val deprecatedKey: String                                         = "deprecated".qualifyAsString
  val publishedKey: String                                          = "published".qualifyAsString

  protected val rdfType: String

  protected def termsFrom(deprecated: Option[Boolean], published: Option[Boolean]): Seq[Json] = {
    val deprecatedTerm = deprecated.map(d => term(deprecatedKey, d))
    val publishedTerm  = published.map(p => term(publishedKey, p))

    Seq(deprecatedTerm, publishedTerm).flatten
  }

  protected def query(terms: Json*): Json = {
    matchAllQuery(Json.arr(terms :+ term(rdfTypeKey, rdfType): _*))
  }

  protected def term(key: String, value: Boolean): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromBoolean(value)
      )
    )
  protected def term(key: String, value: String): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromString(value)
      )
    )

  private def matchAllQuery(filterTerms: Json) =
    Json.obj(
      "query" -> Json.obj(
        "bool" -> Json.obj(
          "must" -> Json.obj(
            "match_all" -> Json.obj()
          ),
          "filter" -> Json.obj(
            "bool" -> Json.obj(
              "must" -> filterTerms
            )
          )
        )
      )
    )

  private def orgTerm(orgId: OrgId): Json          = term("organization".qualifyAsString, orgId.qualifyAsString)
  private def domainTerm(domainId: DomainId): Json = term("domain".qualifyAsString, domainId.qualifyAsString)
  protected def schemaGroupTerm(schemaName: SchemaName): Json =
    term("schemaGroup".qualifyAsString, schemaName.qualifyAsString)

  def list(pagination: Pagination, deprecated: Option[Boolean], published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published): _*))(pagination, sort = defaultSort)
  }

  def list(pagination: Pagination,
           orgId: OrgId,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published) :+ orgTerm(orgId): _*))(pagination,
                                                                                            sort = defaultSort)
  }

  def list(pagination: Pagination,
           domainId: DomainId,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published) :+ domainTerm(domainId): _*))(pagination,
                                                                                                  sort = defaultSort)
  }

}
