package ch.epfl.bluebrain.nexus.kg.indexing

import scala.concurrent.duration.FiniteDuration

/**
  * Exponential backoff retry configuration.
  *
  * @param initialDelay the initial delay when the first failure occurs
  * @param factor       the backoff factor
  * @param maxRetries   the maximum number of retries before giving up
  */
final case class RetryConfig(initialDelay: FiniteDuration, factor: Long, maxRetries: Long)
