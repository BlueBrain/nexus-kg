package ch.epfl.bluebrain.nexus.kg.service.routes

import ch.epfl.bluebrain.nexus.common.types.Err
import ch.epfl.bluebrain.nexus.kg.core.Rejection

/**
  * Enumeration type for rejections returned when a generic rejection occurs.
  */
sealed trait CommonRejections extends Rejection

object CommonRejections {
  /**
    * Signals the inability to find a resource associated to a particular HTTP verb
    *
    * @param supported the collections of supported HTTP verbs for a particular resource
    */
  final case class MethodNotSupported(supported: Seq[String]) extends CommonRejections

  /**
    * Signals the inability to convert the Payload into JSON. It can be due to invalid JSON
    * syntax or due to constrains in the implemented JSON Decoder
    *
    * @param details optional explanation about what went wrong while parsing the Json payload
    */
  final case class WrongOrInvalidJson(details: Option[String])
    extends Err("Invalid json") with CommonRejections

  /**
    * Signals the inability to parse a json structure into a [[ch.epfl.bluebrain.nexus.kg.indexing.filtering.Filter]]
    * instance.
    *
    * @param message a human readable description of the cause
    * @param field   the offending field
    */
  final case class IllegalFilterFormat(override val message: String, field: String)
    extends Err(message) with CommonRejections

}
