package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import akka.http.scaladsl.model.Uri
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextName
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.{And, Eq}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Filter, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries.{domExpr, _}
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QueryResults, QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}

/**
  * Collection of queries.
  *
  * @param queryClient   the client to use for executing the queries
  * @param querySettings the default query settings
  * @tparam F  the monadic effect type
  * @tparam Id the generic type which defines the response's payload
  */
class FilterQueries[F[_], Id](queryClient: SparqlQuery[F], querySettings: QuerySettings)(
    implicit typeExpr: TypeFilterExpr[Id],
    aclExpr: AclSparqlExpr[Id]) {
  private implicit val stringQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId] = Qualifier.configured[OrgId](querySettings.base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId] =
    Qualifier.configured[DomainId](querySettings.base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] =
    Qualifier.configured[SchemaName](querySettings.base)
  private implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId] =
    Qualifier.configured[SchemaId](querySettings.base)
  private implicit val contextNameQualifier: ConfiguredQualifier[ContextName] =
    Qualifier.configured[ContextName](querySettings.base)

  /**
    * Lists all ids in the system that match the given filter.
    *
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(filter: Filter, pagination: Pagination, term: Option[String])(implicit Q: ConfiguredQualifier[Id],
                                                                         caller: Caller): F[QueryResults[Id]] = {
    val query = FilteredQuery[Id](Filter(typeExpr.apply) and filter.expr, pagination, caller.identities, term)
    queryClient[Id](querySettings.index, query, scored = term.isDefined)
  }

  /**
    * Lists all ids in the system within the specified organization that match the given filter.
    *
    * @param org        the organization filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(org: OrgId, filter: Filter, pagination: Pagination, term: Option[String])(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] =
    list(Filter(orgExpr(org)) and filter.expr, pagination, term)

  /**
    * Lists all ids in the system within the specified domain that match the given filter.
    *
    * @param dom        the domain filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(dom: DomainId, filter: Filter, pagination: Pagination, term: Option[String])(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] =
    list(Filter(domExpr(dom)) and filter.expr, pagination, term)

  /**
    * Lists all ids in the system within the specified domain and that have the specified schema name that match
    * the given filter.
    *
    * @param schemaName the schema name filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(schemaName: SchemaName, filter: Filter, pagination: Pagination, term: Option[String])(
      implicit Q: ConfiguredQualifier[Id],
      schemaNameFilter: SchemaNameFilterExpr[Id],
      caller: Caller): F[QueryResults[Id]] =
    list(Filter(schemaNameFilter(schemaName)) and filter.expr, pagination, term)

  /**
    * Lists all ids in the system within the specified context and that have the specified context name that match
    * the given filter.
    *
    * @param contextName the context name filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(contextName: ContextName, filter: Filter, pagination: Pagination, term: Option[String])(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] =
    list(Filter(ComparisonExpr(Eq, UriPath(contextGroupKey), UriTerm(contextName qualify))) and filter.expr,
         pagination,
         term)

  /**
    * Lists all ids in the system conformant to the specified schema that match the given filter.
    *
    * @param schema     the schema filter
    * @param filter     the filter expression to be applied
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def list(schema: SchemaId, filter: Filter, pagination: Pagination, term: Option[String])(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] =
    list(Filter(schemaExpr(schema)) and filter.expr, pagination, term)

  /**
    * Lists all outgoing ids linked to the if identified by ''id'' that match the given filter.
    *
    * @param id         the selected id (this)
    * @param filter     the filter to apply to outgoing ids
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def outgoing(id: Id, filter: Filter, pagination: Pagination, term: Option[String] = None)(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] = {
    val query = FilteredQuery
      .outgoing[Id](id.qualify, Filter(typeExpr.apply) and filter.expr, pagination, caller.identities, term)
    queryClient[Id](querySettings.index, query, scored = term.isDefined)
  }

  /**
    * Lists all incoming ids linked to the id identified by ''id'' that match the given filter.
    *
    * @param id         the selected id (this)
    * @param filter     the filter to apply to incoming id
    * @param pagination the pagination values
    * @param term       the optional full text search term
    */
  def incoming(id: Id, filter: Filter, pagination: Pagination, term: Option[String] = None)(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] = {
    val query = FilteredQuery
      .incoming[Id](id.qualify, Filter(typeExpr.apply) and filter.expr, pagination, caller.identities, term)
    queryClient[Id](querySettings.index, query, scored = term.isDefined)
  }

}

object FilterQueries {

  /**
    * Constructs a [[FilterQueries]]
    *
    * @param queryClient   the client to use for executing the queries
    * @param querySettings the default query settings
    * @tparam F  the monadic effect type
    * @tparam Id the generic type which defines the response's payload
    * @return an instance of [[FilterQueries]]
    */
  final def apply[F[_], Id](queryClient: SparqlQuery[F], querySettings: QuerySettings)(
      implicit typeExpr: TypeFilterExpr[Id],
      aclExpr: AclSparqlExpr[Id]): FilterQueries[F, Id] =
    new FilterQueries(queryClient, querySettings)

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
    def and(expr: Expr): Filter = {
      expr match {
        case LogicalExpr(And, exprList) =>
          filter.expr match {
            case LogicalExpr(And, exprs) => Filter(LogicalExpr(And, exprs ++ exprList))
            case other                   => Filter(LogicalExpr(And, other +: exprList))
          }
        case _ =>
          filter.expr match {
            case LogicalExpr(And, exprs) => Filter(LogicalExpr(And, expr +: exprs))
            case other                   => Filter(LogicalExpr(And, List(expr, other)))
          }
      }
    }

    /**
      * Constructs a new filter based on ''this'' filter by adding the argument filter expression (if defined) to the expressions
      * defined in ''this'' filter using the [[And]] logical operator.
      *
      * @param exprOpt the optional expression to add to ''this'' filter.
      */
    def and(exprOpt: Option[Expr]): Filter =
      exprOpt.map(and).getOrElse(filter)
  }

  /**
    * Constructs a new filter based on an optional filter and optional deprecation parameter.
    *
    * @param deprecatedOpt the optional deprecated field
    * @param filterOps     the optional filter
    * @param baseVoc       the nexus core vocabulary base
    */
  def filterFrom(deprecatedOpt: Option[Boolean], filterOps: Option[Filter], baseVoc: Uri): Filter =
    deprecated(deprecatedOpt, baseVoc) and filterOps.map(_.expr)

  /**
    * Constructs a new filter based on an optional deprecation parameter.
    *
    * @param deprecatedOps the optional deprecated field
    * @param baseVoc       the nexus core vocabulary base
    */
  def deprecated(deprecatedOps: Option[Boolean], baseVoc: Uri): Filter =
    Filter(deprecatedOrNoop(deprecatedOps, baseVoc))

  private def deprecatedOrNoop(deprecated: Option[Boolean], baseVoc: Uri): Expr = {
    deprecated
      .map { value =>
        val depr = "deprecated".qualifyWith(baseVoc)
        ComparisonExpr(Op.Eq, UriPath(depr), LiteralTerm(value.toString))
      }
      .getOrElse(NoopExpr)
  }

  private def orgExpr(org: OrgId)(implicit qual: ConfiguredQualifier[String], orgQ: ConfiguredQualifier[OrgId]): Expr =
    ComparisonExpr(Eq, UriPath("organization" qualify), UriTerm(org qualify))

  private def domExpr(dom: DomainId)(implicit qual: ConfiguredQualifier[String],
                                     domQ: ConfiguredQualifier[DomainId]): Expr =
    ComparisonExpr(Eq, UriPath("domain" qualify), UriTerm(dom qualify))

  private def schemaExpr(schema: SchemaId)(implicit qual: ConfiguredQualifier[String],
                                           schemaQ: ConfiguredQualifier[SchemaId]): Expr =
    ComparisonExpr(Eq, UriPath("schema" qualify), UriTerm(schema qualify))

}
