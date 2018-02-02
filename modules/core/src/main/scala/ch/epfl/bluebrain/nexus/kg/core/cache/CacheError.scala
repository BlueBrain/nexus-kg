package ch.epfl.bluebrain.nexus.kg.core.cache

import ch.epfl.bluebrain.nexus.commons.types.Err

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed trait CacheError extends Err

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object CacheError {
  final case object TypeError                  extends Err("Error on attempting to cast cache value") with CacheError
  final case object EmptyKey                   extends Err("Empty key") with CacheError
  final case object TimeoutError               extends Err("Timeout error while waiting for the actor to respond") with CacheError
  final case class UnknownError(th: Throwable) extends Err("Unknown error") with CacheError
  final case class UnexpectedReply(value: Any) extends Err("Unexpected reply from the actor") with CacheError

}
