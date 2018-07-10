package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.commons.types.Err

/**
  * Enumeration of runtime errors.
  *
  * @param msg a description of the error
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class RuntimeErr(msg: String) extends Err(msg) with Product with Serializable

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object RuntimeErr {

  /**
    * Signals an internal timeout.
    *
    * @param msg a descriptive message on the operation that timed out
    */
  final case class OperationTimedOut(msg: String) extends RuntimeErr(msg)

  /**
    * Signals an illegal event type received from the Kafka topic.
    *
    * @param eventType the offending type
    */
  final case class IllegalEventType(eventType: String, expected: String)
      extends RuntimeErr(s"Illegal event type '$eventType', expected prefix: '$expected'")

}
