package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilderStages._



/**
  * Trait which defines the method which support aggregations operations on a query
  */
private sealed trait AggregationQueryBuilder extends QueryBuilder {

  def groupBy(fields: String*): Aggregation = new Aggregation(params.copy(group = params.group union fields))

  def pagination(p: Pagination): Aggregation = new Aggregation(params.copy(pagination = Some(p)))

  def total(totalCount: TotalCount): Aggregation = new Aggregation(params.copy(total = Some(totalCount)))

}

/**
  * Trait which defines the methods which support where clause
  */
private sealed trait SupportWhere {
  def params: QueryParams

  def where(values: Where[WhereField]): Wheres = new Wheres(params.copy(where = values +: params.where))

  def where(values: Option[Where[WhereField]]): Wheres = values.map(where(_)).getOrElse(new Wheres(params))
}

object QueryBuilderStages {

  /**
    * Holds the prefixes of a query and defines the
    * available actions during the prefix step
    *
    * @param fields the defined prefix mappings
    */
  private[query] class PrefixMappings(val fields: PrefixMapping*) {

    def prefix(prefix: PrefixMapping) = new PrefixMappings((prefix +: fields): _*)

    def select(values: SelectField*) = selects(false, values: _*)

    def selectDistinct(values: SelectField*) = selects(true, values: _*)

    private def selects(distinct: Boolean, values: SelectField*) =
      new Selects(QueryParams(fields, values, distinct))

  }

  /**
    * Holds the select fields of a query and defines the
    * available actions during the select step
    */
  private[query] class Selects(val params: QueryParams) extends AggregationQueryBuilder with SupportWhere {

    def subQuery[A <: QueryBuilder](q: QueryBuilder) = {
      val subQueries = new SelectQueryBuilder(q.params) +: params.subQueries
      new Selects(params.copy(subQueries = subQueries))
    }

    def union[A <: QueryBuilder](q: QueryBuilder) = {
      val unions = new SelectQueryBuilder(q.params) +: params.unions
      new Selects(params.copy(unions = unions))
    }
  }

  /**
    * Holds the where triple of a query and defines the
    * available actions during the where step
    */
  private[query] class Wheres(val params: QueryParams) extends AggregationQueryBuilder with SupportWhere {

    def filter(filter: String): Aggregation =
      if(filter.trim.isEmpty) new Aggregation(params)
      else new Aggregation(params.copy(filter = Some(filter)))

  }

  /**
    * Holds the fields for the aggregation steps and defines the
    * available actions during the aggregation step
    */
  private[query] class Aggregation(val params: QueryParams) extends AggregationQueryBuilder

}