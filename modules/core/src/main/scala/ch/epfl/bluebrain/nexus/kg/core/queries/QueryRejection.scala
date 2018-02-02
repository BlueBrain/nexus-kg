package ch.epfl.bluebrain.nexus.kg.core.queries

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait QueryRejection extends Rejection

object QueryRejection {

  /**
    * Signals that an operation on a query cannot be performed due to the fact that the referenced query
    * does not exists.
    */
  final case object QueryDoesNotExist extends QueryRejection

  /**
    * Signals the failure to create a new query due to an invalid ''id'' provided.
    *
    * @param id the provided identifier
    */
  final case class InvalidQueryId(id: String) extends QueryRejection

}
