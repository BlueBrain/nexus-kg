package ch.epfl.bluebrain.nexus.kg.resources

import cats.syntax.show._

/**
  * Enumeration of resource rejection types.
  *
  * @param msg a descriptive message of the rejection
  */
sealed abstract class Rejection(val msg: String) extends Product with Serializable

object Rejection {

  /**
    * Signals an attempt to interact with a resource that doesn't exist.
    *
    * @param ref a reference to the resource
    */
  final case class ResourceNotFound(ref: Ref) extends Rejection(s"Resource '${ref.show}' not found.")

}
