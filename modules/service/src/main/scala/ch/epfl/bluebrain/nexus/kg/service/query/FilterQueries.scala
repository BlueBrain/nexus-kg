package ch.epfl.bluebrain.nexus.kg.service.query

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, onSuccess}
import akka.http.scaladsl.server.Route
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.{Where, WhereField}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.{FullTextSearchQuery, QueryBuilder, QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.indexing.query.IndexingVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.service.query.FilterQueries.QueryWithPagination
import io.circe.Encoder
import io.circe.generic.auto._
import kamon.akka.http.KamonTraceDirectives.extract
import org.apache.jena.query.Query

import scala.concurrent.Future

/**
  * Build specific queries for filtering endpoints structure
  *
  * @param queryClient   the sparql query
  * @param querySettings query parameters form settings
  */
class FilterQueries[A](queryClient: SparqlQuery[Future], querySettings: QuerySettings) {

  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](querySettings.nexusVocBase)

  /**
    * Query for listing any organization
    *
    * @param deprecated the optionally provided deprecated filter
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def listingQuery(deprecated: Option[Boolean], pagination: Pagination): QueryWithPagination =
    query(None, None, None, None, deprecated, None, pagination)

  /**
    * Query for listing given an organization
    *
    * @param orgId      the provided organization id
    * @param deprecated the optionally provided deprecated filter
    * @param published  the optionally provided published filter
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def listingQuery(orgId: OrgId, deprecated: Option[Boolean], published: Option[Boolean], pagination: Pagination): QueryWithPagination =
    query(Some(orgId.id), None, None, None, deprecated, published, pagination)

  /**
    * Query for listing given a domain
    *
    * @param domainId   the provided domain id
    * @param deprecated the optionally provided deprecated filter
    * @param published  the optionally provided published filter
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def listingQuery(domainId: DomainId, deprecated: Option[Boolean], published: Option[Boolean], pagination: Pagination): QueryWithPagination =
    query(Some(domainId.orgId.id), Some(domainId.id), None, None, deprecated, published, pagination)

  /**
    * Query for listing given a domainId and a schema name
    *
    * @param domainId   the provided domain id
    * @param name       the provided schema name
    * @param deprecated the optionally provided deprecated filter
    * @param published  the optionally provided published filter
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def listingQuery(domainId: DomainId, name: String, deprecated: Option[Boolean], published: Option[Boolean], pagination: Pagination): QueryWithPagination =
    query(Some(domainId.orgId.id), Some(domainId.id), Some(name), None, deprecated, published, pagination)

  /**
    * Query for listing given a domainId and a schema
    *
    * @param domainId   the provided domain id
    * @param schema     the provided schema
    * @param deprecated the optionally provided deprecated filter
    * @param published  the optionally provided published filter
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def listingQuery(domainId: DomainId, schema: SchemaId, deprecated: Option[Boolean], published: Option[Boolean], pagination: Pagination): QueryWithPagination =
    query(Some(domainId.orgId.id), Some(domainId.id), Some(schema.name), Some(schema.version), deprecated, published, pagination)

  /**
    * Query for full text search endpoint
    *
    * @param term       the search term
    * @param pagination the query pagination
    * @return the query for those provided parameters
    */
  def fullTextSearchQuery(term: String, pagination: Pagination): QueryWithPagination =
    QueryWithPagination(FullTextSearchQuery(term, pagination).build(), pagination)

  private def query(orgId: Option[String],
    domainId: Option[String],
    schemaName: Option[String],
    version: Option[Version],
    deprecated: Option[Boolean],
    published: Option[Boolean],
    pagination: Pagination): QueryWithPagination = {

    val q = QueryBuilder.select(subject)
      .where("organization".qualify -> "org")
      .where(addOptionalWhere("domain", domainId))
      .where(addOptionalWhere("schema", schemaName))
      .where(addOptionalWhere("version", version))
      .where(addOptionalWhere("deprecated", deprecated))
      .where(addOptionalWhere("published", published))
      .filter(filterText(orgId, domainId, schemaName, version, deprecated, published))
      .pagination(pagination)
      .total(subject -> total).build()
    QueryWithPagination(q, pagination)
  }


  private def addOptionalWhere[B](key: String, field: Option[B]): Option[Where[WhereField]] = field.map(_ => key.qualify -> key)

  def buildResponse(q: QueryWithPagination)(implicit Q: ConfiguredQualifier[A], R: Encoder[UnscoredQueryResult[A]], S: Encoder[ScoredQueryResult[A]]): Route =
    extract(_.request.uri) { uri =>
      onSuccess(queryClient[A](querySettings.index, q.query)) { result =>
        complete(StatusCodes.OK -> LinksQueryResults(result, q.pagination, uri))
      }
    }

  private def filterText(orgId: Option[String], domainId: Option[String], schemaName: Option[String], schemaVersion: Option[Version], deprecated: Option[Boolean], published: Option[Boolean]) =
    StringBuilder.newBuilder
      .append(orgId.map(v => s"""?org = "$v"""").getOrElse(""))
      .append(domainId.map(v => s""" && ?domain = "$v"""").getOrElse(""))
      .append(schemaName.map(v => s""" && ?schema = "$v"""").getOrElse(""))
      .append(schemaVersion.map(v => s""" && ?version = "${v.show}"""").getOrElse(""))
      .append(deprecated.map(v => s" && ?deprecated = $v").getOrElse(""))
      .append(published.map(v => s" && ?published = $v").getOrElse("")).toString().dropWhile(_ == '&').trim

  implicit class BuildResponseOps(q: QueryWithPagination)(implicit Q: ConfiguredQualifier[A], R: Encoder[UnscoredQueryResult[A]], S: Encoder[ScoredQueryResult[A]]) {
    def response = buildResponse(q)
  }

}

object FilterQueries {

  /**
    * Data type which holds both the SPARQL query and the pagination used.
    *
    * @param query      the SPAQRL query
    * @param pagination the query pagination
    */
  case class QueryWithPagination(query: Query, pagination: Pagination)

}
