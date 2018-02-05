package ch.epfl.bluebrain.nexus.kg.core.queries

import io.circe.Decoder

/**
  * Enumeration type for supported resources.
  */
trait QueryResource extends Product with Serializable

object QueryResource {

  /**
    * Organization resources
    */
  final case object Organizations extends QueryResource

  /**
    * Domain resources
    */
  final case object Domains extends QueryResource

  /**
    * Schema resources
    */
  final case object Schemas extends QueryResource

  /**
    * Context resources
    */
  final case object Contexts extends QueryResource

  /**
    * Instance resources
    */
  final case object Instances extends QueryResource

  def fromString(value: String): Option[QueryResource] = value match {
    case "organizations" => Some(Organizations)
    case "domains"       => Some(Domains)
    case "schemas"       => Some(Schemas)
    case "contexts"      => Some(Contexts)
    case "instances"     => Some(Instances)
    case _               => None
  }

  implicit final val queryResourceDecoder: Decoder[QueryResource] =
    Decoder.decodeString.emap(v => fromString(v).toRight(s"Could not find QueryResource for '$v'"))
}
