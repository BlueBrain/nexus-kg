package ch.epfl.bluebrain.nexus.kg

import cats.MonadError
import cats.effect.Async
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr

object instances {

  /**
    * Generates a MonadError for [[KgError]]
    *
    * @tparam F the effect type
    */
  implicit def kgErrorMonadError[F[_]](implicit F: Async[F]): MonadError[F, KgError] =
    new MonadError[F, KgError] {
      override def handleErrorWith[A](fa: F[A])(f: KgError => F[A]): F[A] = F.recoverWith(fa) {
        case t: KgError => f(t)
      }
      override def raiseError[A](e: KgError): F[A]                     = F.raiseError(e)
      override def pure[A](x: A): F[A]                                 = F.pure(x)
      override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]         = F.flatMap(fa)(f)
      override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] = F.tailRecM(a)(f)
    }

  /**
    * Generates a MonadError for [[RetriableErr]]
    *
    * @tparam F the effect type
    */
  implicit def retriableMonadError[F[_]](implicit F: Async[F]): MonadError[F, RetriableErr] =
    new MonadError[F, RetriableErr] {
      override def handleErrorWith[A](fa: F[A])(f: RetriableErr => F[A]): F[A] = F.recoverWith(fa) {
        case t: RetriableErr => f(t)
      }
      override def raiseError[A](e: RetriableErr): F[A]                = F.raiseError(e)
      override def pure[A](x: A): F[A]                                 = F.pure(x)
      override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]         = F.flatMap(fa)(f)
      override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] = F.tailRecM(a)(f)
    }
}
