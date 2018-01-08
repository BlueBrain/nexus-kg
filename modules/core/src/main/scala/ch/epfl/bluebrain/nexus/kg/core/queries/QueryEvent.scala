package ch.epfl.bluebrain.nexus.kg.core.queries

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for all events that are emitted for queries.
  */
sealed trait QueryEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the query
    */
  def id: QueryId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object QueryEvent {

  /**
    * Evidence that a query has been created.
    *
    * @param id    the unique identifier of the query
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the initial value of the query
    */
  final case class QueryCreated(id: QueryId, rev: Long, meta: Meta, value: Json) extends QueryEvent

  /**
    * Evidence that a query has been updated.
    *
    * @param id    the unique identifier of the query
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new value of the query
    */
  final case class QueryUpdated(id: QueryId, rev: Long, meta: Meta, value: Json) extends QueryEvent

}
