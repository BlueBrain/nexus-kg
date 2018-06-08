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

  /**
    * Signals that a resource has an illegal context value.
    *
    * @param ref a reference to the resource
    */
  final case class IllegalContextValue(ref: Ref)
      extends Rejection(s"Resource '${ref.show}' has an illegal context value.")

  /**
    * Signals that the system is unable to select a primary node from a resource graph.
    *
    * @param ref a reference to the resource
    */
  final case class UnableToSelectResourceId(ref: Ref)
      extends Rejection(s"Resource '${ref.show}' is not entity centric, unable to select primary node.")
}
