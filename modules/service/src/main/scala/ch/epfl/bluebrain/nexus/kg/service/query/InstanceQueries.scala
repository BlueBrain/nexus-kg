package ch.epfl.bluebrain.nexus.kg.service.query

import akka.http.scaladsl.model.Uri
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, LogicalExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.{And, Eq}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Filter}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilteredQuery
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QueryResults, QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.query.InstanceQueries._

import scala.concurrent.Future

/**
  * Collection of queries specific to instances.
  *
  * @param queryClient   the client to use for executing the queries
  * @param querySettings the default query settings
  * @param base          the base uri of the service
  */
class InstanceQueries(queryClient: SparqlQuery[Future], querySettings: QuerySettings, base: Uri) {

  private implicit val instanceIdQualifier: ConfiguredQualifier[InstanceId] =
    Qualifier.configured[InstanceId](base)

  private implicit val stringQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)

  /**
    * Lists all instances in the system that match the given filter.
    *
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    */
  def list(filter: Filter, pagination: Pagination): Future[QueryResults[InstanceId]] = {
    val query = FilteredQuery(filter, pagination)
    queryClient[InstanceId](querySettings.index, query, scored = false)
  }

  /**
    * Lists all instances in the system within the specified organization that match the given filter.
    *
    * @param org        the organization filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    */
  def list(org: OrgId, filter: Filter, pagination: Pagination): Future[QueryResults[InstanceId]] =
    list(filter and orgExpr(org), pagination)

  /**
    * Lists all instances in the system within the specified domain that match the given filter.
    *
    * @param dom        the domain filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    */
  def list(dom: DomainId, filter: Filter, pagination: Pagination): Future[QueryResults[InstanceId]] =
    list(
      filter
        and orgExpr(dom.orgId)
        and domExpr(dom),
      pagination)

  /**
    * Lists all instances in the system within the specified domain and that have the specified schema name that match
    * the given filter.
    *
    * @param dom        the domain filter
    * @param schemaName the schema name filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    */
  def list(dom: DomainId, schemaName: String, filter: Filter, pagination: Pagination): Future[QueryResults[InstanceId]] =
    list(
      filter
        and orgExpr(dom.orgId)
        and domExpr(dom)
        and schemaNameExpr(schemaName),
      pagination)

  /**
    * Lists all instances in the system conformant to the specified schema that match the given filter.
    *
    * @param schema     the schema filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    */
  def list(schema: SchemaId, filter: Filter, pagination: Pagination): Future[QueryResults[InstanceId]] =
    list(
      filter
        and orgExpr(schema.domainId.orgId)
        and domExpr(schema.domainId)
        and schemaNameExpr(schema.name)
        and schemaVerExpr(schema.version),
      pagination)
}

object InstanceQueries {

  /**
    * Syntactic sugar for composing filters using the [[And]] logical operator.
    */
  implicit class FilterOps(filter: Filter) {
    /**
      * Constructs a new filter based on ''this'' filter by adding the argument filter expression to the expressions
      * defined in ''this'' filter using the [[And]] logical operator.
      *
      * @param expr the expression to add to ''this'' filter.
      */
    def and(expr: Expr): Filter = filter.expr match {
      case LogicalExpr(And, exprs) => Filter(LogicalExpr(And, expr +: exprs))
      case other                   => Filter(LogicalExpr(And, List(expr, other)))
    }
  }

  private def orgExpr(org: OrgId)(implicit qual: ConfiguredQualifier[String]): Expr =
    ComparisonExpr(Eq, UriTerm("organization" qualify), lit(org.id))

  private def domExpr(dom: DomainId)(implicit qual: ConfiguredQualifier[String]): Expr =
    ComparisonExpr(Eq, UriTerm("domain" qualify), lit(dom.id))

  private def schemaNameExpr(schemaName: String)(implicit qual: ConfiguredQualifier[String]): Expr =
    ComparisonExpr(Eq, UriTerm("schema" qualify), lit(schemaName))

  private def schemaVerExpr(version: Version)(implicit qual: ConfiguredQualifier[String]): Expr =
    ComparisonExpr(Eq, UriTerm("version" qualify), lit(version.show))

  private def lit(value: String): LiteralTerm =
    LiteralTerm(s""""$value"""")
}