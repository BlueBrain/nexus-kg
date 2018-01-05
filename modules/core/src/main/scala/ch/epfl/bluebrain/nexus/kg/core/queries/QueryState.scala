package ch.epfl.bluebrain.nexus.kg.core.queries

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for possible states of a query.
  */
sealed trait QueryState extends Product with Serializable

object QueryState {

  /**
    * Initial state of all queries.
    */
  final case object Initial extends QueryState

  /**
    * State used for all queries that have been created and later possibly updated.
    *
    * @param id         the unique identifier for the query
    * @param rev        the last revision number
    * @param meta       the metadata associated to this query
    * @param value      the current json value
    */
  final case class Current(id: QueryId, rev: Long, meta: Meta, value: Json) extends QueryState

}