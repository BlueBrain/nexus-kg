package ch.epfl.bluebrain.nexus.kg.core.rejections

import ch.epfl.bluebrain.nexus.commons.types.{Err, Rejection}

/**
  * Enumeration type for rejections returned when a generic rejection occurs.
  */
sealed trait CommonRejections extends Rejection

object CommonRejections {

  /**
    * Signals the inability to convert the requested query parameter.
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalParam(override val message: String) extends Err(message) with CommonRejections

  /**
    * Signals the inability to convert the requested payload.
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class IllegalPayload(override val message: String, details: Option[String])
      extends Err(message)
      with CommonRejections

  /**
    * Signals the inability to connect to an underlying service to perform a request
    *
    * @param message a human readable description of the cause
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class DownstreamServiceError(override val message: String) extends Err(message) with CommonRejections

  /**
    * Signals the requirement of a parameter to be present
    *
    * @param message a human readable description of the cause
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class MissingParameter(override val message: String) extends Err(message) with CommonRejections

}
