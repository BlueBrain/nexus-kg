package ch.epfl.bluebrain.nexus.kg.indexing

import cats.MonadError
import cats.effect.Timer
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

object Retry {

  /**
    * Retries the computation in ''F[_]'' with exponential backoff.
    *
    * @param fa           the computation to be retried
    * @param initialDelay the initial sleep delay when the first error occurs
    * @param maxRetries   the maximum number of retries before giving up
    */
  def retryWithBackOff[F[_], A](
      fa: F[A],
      initialDelay: FiniteDuration,
      factor: Long,
      maxRetries: Long
  )(implicit timer: Timer[F], F: MonadError[F, Throwable]): F[A] =
    fa.handleErrorWith { error =>
      if (maxRetries > 0)
        timer.sleep(initialDelay) *> retryWithBackOff(fa, initialDelay * factor, factor, maxRetries - 1)
      else
        F.raiseError(error)
    }

  /**
    * Retries the computation in ''F[_]'' with exponential backoff.
    *
    * @param fa  the computation to be retried
    * @param cfg the retry configuration
    */
  def retryWithBackOff[F[_]: Timer, A](fa: F[A], cfg: RetryConfig)(implicit F: MonadError[F, Throwable]): F[A] =
    retryWithBackOff(fa, cfg.initialDelay, cfg.factor, cfg.maxRetries)
}
