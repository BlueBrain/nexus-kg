package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, _}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextName
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.{ComparisonExpr, LogicalExpr}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.{And, Eq}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.UriTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Expr, Filter}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}

/**
  * Collection of queries.
  *
  * @param queryClient   the client to use for executing the queries
  * @param querySettings the default query settings
  * @tparam F  the monadic effect type
  * @tparam Id the generic type which defines the response's payload
  */
class FilterQueries[F[_], Id](queryClient: SparqlQuery[F])(implicit querySettings: QuerySettings,
                                                           typeExpr: TypeFilterExpr[Id],
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
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def list(query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                        caller: Caller): F[QueryResults[Id]] = {
    val queryString = FilteredQuery[Id](query, pagination, caller.identities)
    queryClient[Id](querySettings.index, queryString, scored = query.q.isDefined)
  }

  /**
    * Lists all ids in the system within the specified organization that match the given filter.
    *
    * @param org        the organization filter
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def list(org: OrgId, query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                                    caller: Caller): F[QueryResults[Id]] = {
    val filter = Filter(ComparisonExpr(Eq, UriPath("organization" qualify), UriTerm(org qualify)))
    list(query.copy(filter = filter and query.filter.expr), pagination)
  }

  /**
    * Lists all ids in the system within the specified domain that match the given filter.
    *
    * @param dom        the domain filter
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def list(dom: DomainId, query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                                       caller: Caller): F[QueryResults[Id]] = {
    val filter = Filter(ComparisonExpr(Eq, UriPath("domain" qualify), UriTerm(dom qualify)))
    list(query.copy(filter = filter and query.filter.expr), pagination)
  }

  /**
    * Lists all ids in the system within the specified domain and that have the specified schema name that match
    * the given filter.
    *
    * @param schemaName the schema name filter
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def list(schemaName: SchemaName, query: QueryPayload, pagination: Pagination)(
      implicit Q: ConfiguredQualifier[Id],
      schemaNameFilter: SchemaNameFilterExpr[Id],
      caller: Caller): F[QueryResults[Id]] = {
    val filter = Filter(schemaNameFilter(schemaName))
    list(query.copy(filter = filter and query.filter.expr), pagination)
  }

  /**
    * Lists all ids in the system within the specified context and that have the specified context name that match
    * the given filter.
    *
    * @param contextName the context name filter
    * @param query      the query payload to be applied
    * @param pagination  the pagination values
    */
  def list(contextName: ContextName, query: QueryPayload, pagination: Pagination)(
      implicit Q: ConfiguredQualifier[Id],
      caller: Caller): F[QueryResults[Id]] = {
    val filter = Filter(ComparisonExpr(Eq, UriPath(contextGroupKey), UriTerm(contextName qualify)))
    list(query.copy(filter = filter and query.filter.expr), pagination)
  }

  /**
    * Lists all ids in the system conformant to the specified schema that match the given filter.
    *
    * @param schema     the schema filter
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def list(schema: SchemaId, query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                                          caller: Caller): F[QueryResults[Id]] = {
    val filter = Filter(ComparisonExpr(Eq, UriPath("schema" qualify), UriTerm(schema qualify)))
    list(query.copy(filter = filter and query.filter.expr), pagination)
  }

  /**
    * Lists all outgoing ids linked to the if identified by ''id'' that match the given filter.
    *
    * @param id         the selected id (this)
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def outgoing(id: Id, query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                                    caller: Caller): F[QueryResults[Id]] = {
    val queryString = FilteredQuery
      .outgoing[Id](query, id.qualify, pagination, caller.identities)
    queryClient[Id](querySettings.index, queryString, scored = query.q.isDefined)
  }

  /**
    * Lists all incoming ids linked to the id identified by ''id'' that match the given filter.
    *
    * @param id         the selected id (this)
    * @param query      the query payload to be applied
    * @param pagination the pagination values
    */
  def incoming(id: Id, query: QueryPayload, pagination: Pagination)(implicit Q: ConfiguredQualifier[Id],
                                                                    caller: Caller): F[QueryResults[Id]] = {
    val queryString = FilteredQuery
      .incoming[Id](query, id.qualify, pagination, caller.identities)
    queryClient[Id](querySettings.index, queryString, scored = query.q.isDefined)
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
  final def apply[F[_], Id](queryClient: SparqlQuery[F])(implicit
                                                         querySettings: QuerySettings,
                                                         typeExpr: TypeFilterExpr[Id],
                                                         aclExpr: AclSparqlExpr[Id]): FilterQueries[F, Id] =
    new FilterQueries(queryClient)

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
}
