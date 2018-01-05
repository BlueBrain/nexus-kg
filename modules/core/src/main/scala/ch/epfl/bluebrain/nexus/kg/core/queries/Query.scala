package ch.epfl.bluebrain.nexus.kg.core.queries

import io.circe.Json

/**
  * Data type representing the current state of a query.
  *
  * @param id         the unique identifier for the query
  * @param rev        the current revision of the query
  * @param value      the value of the query as a json value
  */
final case class Query(id: QueryId, rev: Long, value: Json)