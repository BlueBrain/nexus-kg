package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder._

/**
  * Holds all the provided parameters to build a Blazegraph query.
  *
  * @param prefix     the parameters for the prefix step
  * @param select     the parameters for the select step
  * @param distinct   the parameter which defines if the select is distinct or not
  * @param where      the parameters for the where step
  * @param filter     the parameters for the filter step
  * @param group      the parameters for the groupBy step
  * @param subQueries the parameters for the subquery
  * @param unions     the parameters for the union step
  * @param pagination the parameters for the pagination step
  * @param withs      the parameters for the with step
  * @param includes   the included variables stored with the with clause
  * @param order      the parameters for the ORDER BY clause
  * @param optional   the parameters for the OPTIONAL clause
  */
private[builder] final case class QueryParams(
  prefix: Seq[PrefixMapping] = Seq.empty,
  select: Seq[Field] = Seq.empty,
  distinct: Boolean = false,
  where: Seq[Triple[TripleContent]] = Seq.empty,
  filter: Option[String] = None,
  group: Seq[Field] = Seq.empty,
  subQueries: Seq[QueryParams] = Seq.empty,
  unions: Seq[QueryParams] = Seq.empty,
  pagination: Option[Pagination] = None,
  withs: Seq[(QueryParams, String)] = Seq.empty,
  includes: Seq[String] = Seq.empty,
  order: Seq[Order] = Seq.empty,
  optional: Seq[Triple[TripleContent]] = Seq.empty
)