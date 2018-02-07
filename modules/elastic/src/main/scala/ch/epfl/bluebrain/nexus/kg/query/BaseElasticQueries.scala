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

/**
  * Base class implementing common Elastic Search queries
  *
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  * @tparam Id            type of the ID that's returned
  */
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

  /**
    * RDF type used to filter entities
    */
  protected val rdfType: String

  /**
    * Created filter terms from `deprecated` and `published` parmeters
    * @param deprecated deprecated filter value
    * @param published  published filter value
    * @return `Json` object representing the filter
    */
  protected def termsFrom(deprecated: Option[Boolean], published: Option[Boolean]): Seq[Json] = {
    val deprecatedTerm = deprecated.map(d => term(deprecatedKey, d))
    val publishedTerm  = published.map(p => term(publishedKey, p))

    Seq(deprecatedTerm, publishedTerm).flatten
  }

  /**
    * Embed given `terms` inside a Elasitc Search Query and add RDF type term
    * @param terms  filter terms
    * @return `Json` object representing the query
    */
  protected def query(terms: Json*): Json = {
    matchAllQuery(Json.arr(terms :+ term(rdfTypeKey, rdfType): _*))
  }

  /**
    * Create a boolean term for Elastic Search filter
    * @param key    field on which to apply the filter
    * @param value  filter value
    * @return `Json` object representing the term
    */
  protected def term(key: String, value: Boolean): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromBoolean(value)
      )
    )

  /**
    ** Create a string term for Elastic Search filter
    * @param key    field on which to apply the filter
    * @param value  filter value
    * @return `Json` object representing the term
    */
  protected def term(key: String, value: String): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromString(value)
      )
    )

  /**
    * Create a term for SchemaGroup filter
    * @param schemaName name of the schema to filter by
    * @return `Json` object representing the term
    */
  protected def schemaGroupTerm(schemaName: SchemaName): Json =
    term("schemaGroup".qualifyAsString, schemaName.qualifyAsString)

  private def matchAllQuery(filterTerms: Json) =
    Json.obj(
      "query" -> Json.obj(
        "bool" -> Json.obj(
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

  /**
    * List all objects of a given type
    * @param pagination   pagination object
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @return query results
    */
  def list(pagination: Pagination, deprecated: Option[Boolean], published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published): _*))(pagination, sort = defaultSort)
  }

  /**
    * List all objects of a given type within an organization
    * @param pagination   pagination object
    * @param orgId        organization ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @return query results
    *
    */
  def list(pagination: Pagination,
           orgId: OrgId,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published) :+ orgTerm(orgId): _*))(pagination,
                                                                                            sort = defaultSort)
  }

  /**
    * List all objects of a given type within a domain
    * @param pagination   pagination object
    * @param domainId     domain ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @return query results
    *
    */
  def list(pagination: Pagination,
           domainId: DomainId,
           deprecated: Option[Boolean],
           published: Option[Boolean]): F[QueryResults[Id]] = {
    elasticClient.search[Id](query(termsFrom(deprecated, published) :+ domainTerm(domainId): _*))(pagination,
                                                                                                  sort = defaultSort)
  }

}
