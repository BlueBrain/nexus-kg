package ch.epfl.bluebrain.nexus.kg.service.routes

import ch.epfl.bluebrain.nexus.commons.types.Err
import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when a generic rejection occurs.
  */
sealed trait CommonRejections extends Rejection

object CommonRejections {

  /**
    * Signals the inability to parse a json structure into a [[ch.epfl.bluebrain.nexus.kg.indexing.filtering.Filter]]
    * instance.
    *
    * @param message a human readable description of the cause
    * @param field   the offending field
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalFilterFormat(override val message: String, field: String)
      extends Err(message)
      with CommonRejections

  /**
    * Signals the inability to convert a path segment into a [[ch.epfl.bluebrain.nexus.commons.types.Version]]
    *
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalVersionFormat(override val message: String) extends Err(message) with CommonRejections

  /**
    * Signals the inability to convert the requested output format into a valid
    * [[ch.epfl.bluebrain.nexus.kg.service.directives.JsonLdFormat]]
    *
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalOutputFormat(override val message: String) extends Err(message) with CommonRejections

  /**
    * Signals the inability to connect to an underlying service to perform a request
    *
    * @param message a human readable description of the cause
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class DownstreamServiceError(override val message: String) extends Err(message) with CommonRejections

}