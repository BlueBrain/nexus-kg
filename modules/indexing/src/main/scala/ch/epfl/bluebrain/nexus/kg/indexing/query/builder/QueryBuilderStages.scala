package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilderStages._



/**
  * Trait which defines the method which support aggregations operations on a query
  */
private sealed trait AggregationQueryBuilder extends QueryBuilder {

  def groupBy(fields: Field*): Aggregation = new Aggregation(params.copy(group = params.group union fields))

  def pagination(p: Pagination): Aggregation = new Aggregation(params.copy(pagination = Some(p)))

  def orderBy(fields: Order): Aggregation = new Aggregation(params.copy(order = fields +: params.order))
}

/**
  * Trait which defines the methods which support where clause
  */
private sealed trait SupportWhere {
  def params: QueryParams

  def where(values: Triple[TripleContent]): Wheres = new Wheres(params.copy(where = values +: params.where))

  def where(values: Option[Triple[TripleContent]]): Wheres = values.map(where).getOrElse(new Wheres(params))

  def include(name: String): Wheres = new Wheres(params.copy(includes = name +: params.includes))

  def optional(values: Triple[TripleContent]): Wheres = new Wheres(params.copy(optional = values +: params.optional))
}

object QueryBuilderStages {

  /**
    * Holds the prefixes of a query and defines the
    * available actions during the prefix step
    *
    * @param fields the defined prefix mappings
    */
  private[builder] class PrefixMappings(val fields: PrefixMapping*) {

    def prefix(prefix: PrefixMapping) = new PrefixMappings(prefix +: fields: _*)

    def select(values: Field*): Selects = selects(false, values: _*)

    def selectDistinct(values: Field*): Selects = selects(true, values: _*)

    private def selects(distinct: Boolean, values: Field*) =
      new Selects(QueryParams(fields, values, distinct))

  }

  /**
    * Holds the select fields of a query and defines the
    * available actions during the select step
    */
  private[builder] class Selects(val params: QueryParams) extends AggregationQueryBuilder with SupportWhere {

    def subQuery[A <: QueryBuilder](q: QueryBuilder): Selects = {
      val subQueries = q.params +: params.subQueries
      new Selects(params.copy(subQueries = subQueries))
    }

    def union[A <: QueryBuilder](q: QueryBuilder): Selects = {
      val unions = q.params +: params.unions
      new Selects(params.copy(unions = unions))
    }

    def `with`[A <: QueryBuilder](q: QueryBuilder, name: String): Selects = {
      val withs = (q.params -> name) +: params.withs
      new Selects(params.copy(withs = withs))
    }
  }

  /**
    * Holds the where triple of a query and defines the
    * available actions during the where step
    */
  private[builder] class Wheres(val params: QueryParams) extends AggregationQueryBuilder with SupportWhere {

    def filter(filter: String): Aggregation =
      if(filter.trim.isEmpty) new Aggregation(params)
      else new Aggregation(params.copy(filter = Some(filter)))
  }

  /**
    * Holds the fields for the aggregation steps and defines the
    * available actions during the aggregation step
    */
  private[builder] class Aggregation(val params: QueryParams) extends AggregationQueryBuilder

}