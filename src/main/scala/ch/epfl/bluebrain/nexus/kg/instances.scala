package ch.epfl.bluebrain.nexus.kg

import cats.MonadError
import cats.effect.Async
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure

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
    * Generates a MonadError for [[SparqlServerOrUnexpectedFailure]]
    *
    * @tparam F the effect type
    */
  implicit def sparqlErrorMonadError[F[_]](implicit F: Async[F]): MonadError[F, SparqlServerOrUnexpectedFailure] =
    new MonadError[F, SparqlServerOrUnexpectedFailure] {
      override def handleErrorWith[A](fa: F[A])(f: SparqlServerOrUnexpectedFailure => F[A]): F[A] = F.recoverWith(fa) {
        case t: SparqlServerOrUnexpectedFailure => f(t)
      }
      override def raiseError[A](e: SparqlServerOrUnexpectedFailure): F[A] = F.raiseError(e)
      override def pure[A](x: A): F[A]                                     = F.pure(x)
      override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]             = F.flatMap(fa)(f)
      override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B]     = F.tailRecM(a)(f)
    }

  /**
    * Generates a MonadError for [[ElasticSearchServerOrUnexpectedFailure]]
    *
    * @tparam F the effect type
    */
  implicit def elasticErrorMonadError[F[_]](
      implicit F: Async[F]
  ): MonadError[F, ElasticSearchServerOrUnexpectedFailure] =
    new MonadError[F, ElasticSearchServerOrUnexpectedFailure] {
      override def handleErrorWith[A](fa: F[A])(f: ElasticSearchServerOrUnexpectedFailure => F[A]): F[A] =
        F.recoverWith(fa) {
          case t: ElasticSearchServerOrUnexpectedFailure => f(t)
        }
      override def raiseError[A](e: ElasticSearchServerOrUnexpectedFailure): F[A] = F.raiseError(e)
      override def pure[A](x: A): F[A]                                            = F.pure(x)
      override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]                    = F.flatMap(fa)(f)
      override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B]            = F.tailRecM(a)(f)
    }
}
