package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder._

/**
  * Holds all the provided parameters to build a SPARQL query.
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
  * @param total      the optional parameters which defines the field where to take the
  *                   total count from
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
  total: Option[TotalCount] = None)