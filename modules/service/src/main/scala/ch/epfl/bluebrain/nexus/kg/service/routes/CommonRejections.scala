package ch.epfl.bluebrain.nexus.kg.service.routes

import ch.epfl.bluebrain.nexus.kg.core.Rejection

/**
  * Enumeration type for rejections returned when a generic rejection occurs.
  */
sealed trait CommonRejections extends Rejection

object CommonRejections {
  /**
    * Signals the incapability to find a resource associated to a particular HTTP verb
    *
    * @param supported the collections of supported HTTP verbs for a particular resource
    */
  final case class MethodNotSupported(supported: Seq[String]) extends CommonRejections

  /**
    * Signals the incapability to convert the Payload into JSON. It can be due to invalid JSON
    * syntax or due to constrains in the implemented JSON Decoder
    *
    * @param details optional explanation about what went wrong while parsing the Json payload
    */
  final case class WrongOrInvalidJson(details: Option[String]) extends CommonRejections

}
