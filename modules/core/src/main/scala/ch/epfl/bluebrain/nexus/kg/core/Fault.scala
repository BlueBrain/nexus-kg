package ch.epfl.bluebrain.nexus.kg.core

import ch.epfl.bluebrain.nexus.commons.types.Err

/**
  * Top level exception type for the knowledge graph that describes a failure in the system.
  *
  * @param reason the reason why the failure occurred
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class Fault(reason: String) extends Err(reason)

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object Fault {

  /**
    * An unexpected failure within the system typically caused by an unexpected state or change within the system.
    *
    * @param reason the reason why the failure occurred
    */
  final case class Unexpected(reason: String) extends Fault(reason)

  /**
    * A failure that wraps command rejections.
    *
    * @param rejection the rejection returned for a command evaluation
    */
  final case class CommandRejected(rejection: Rejection) extends Fault("Command rejected")

}
