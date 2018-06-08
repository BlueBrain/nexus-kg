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
    * Signals an internal failure where the state of a resource is not the expected state.
    *
    * @param ref a reference to the resource
    */
  final case class UnexpectedState(ref: Ref) extends Rejection(s"Resource '${ref.show}' is in an unexpected state.")

  /**
    * Signals an attempt to interact with a resource that doesn't exist.
    *
    * @param ref a reference to the resource
    */
  final case class NotFound(ref: Ref) extends Rejection(s"Resource '${ref.show}' not found.")

}
