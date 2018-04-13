package ch.epfl.bluebrain.nexus.kg.core.resources

import io.circe.Json

/**
  * Enumeration type for payloads.
  */
sealed trait Payload extends Product with Serializable

object Payload {

  /**
    * A Json payload
    *
    * @param body the json value
    */
  final case class JsonPayload(body: Json) extends Payload

  /**
    * A JsonLD payload
    *
    * @param body the json value
    */
  final case class JsonLDPayload(body: Json) extends Payload

  /**
    * A turtle payload
    *
    * @param body the string value
    */
  final case class TurtlePayload(body: String) extends Payload
}
