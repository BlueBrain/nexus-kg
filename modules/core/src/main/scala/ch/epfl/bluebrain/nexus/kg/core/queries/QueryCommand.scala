package ch.epfl.bluebrain.nexus.kg.core.queries

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import io.circe.Json

/**
  * Enumeration type for commands that apply to queries.
  */
sealed trait QueryCommand extends Product with Serializable {

  /**
    * @return the unique identifier for the query for which this command will be evaluated
    */
  def id: QueryId

  /**
    * @return the metadata associated to this command
    */
  def meta: Meta
}

object QueryCommand {

  /**
    * Command that signals the intent to create a new query.
    *
    * @param id    the unique identifier for the query to be created
    * @param meta  the metadata associated to this command
    * @param value a json representation of the query
    */
  final case class CreateQuery(id: QueryId, meta: Meta, value: Json) extends QueryCommand

  /**
    * Command that signals the intent to update an existing query.
    *
    * @param id    the unique identifier for the query to be created
    * @param rev   the last known revision of the query
    * @param meta  the metadata associated to this command
    * @param value the new json value for the query
    */
  final case class UpdateQuery(id: QueryId, rev: Long, meta: Meta, value: Json) extends QueryCommand

}
