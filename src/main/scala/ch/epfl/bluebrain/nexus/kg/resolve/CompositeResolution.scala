package ch.epfl.bluebrain.nexus.kg.resolve
import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources.{Ref, Resource}

/**
  * Implementation that uses underlying resolutions in defined order to resolve resources.
  * @param  resolutions underlying resolutions
  * @tparam F the resolution effect type
  */
class CompositeResolution[F[_]](resolutions: List[Resolution[F]])(implicit F: Monad[F]) extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = resolutions.foldLeft[F[Option[Resource]]](F.pure(None)) {
    (previousResult, resolution) =>
      previousResult.flatMap {
        case resolved @ Some(_) => F.pure(resolved)
        case None               => resolution.resolve(ref)
      }
  }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    resolutions
      .map(_.resolve(ref))
      .foldLeft[F[List[Resource]]](F.pure(List.empty)) { (resolved, result) =>
        result.flatMap {
          case Some(res) => resolved.map(res :: _)
          case None      => resolved
        }
      }
      .map(_.reverse)
}

object CompositeResolution {

  /**
    * Constructs a [[CompositeResolution]] instance.
    *
    * @param resolutions underlying resolutions
    */
  final def apply[F[_]](resolutions: List[Resolution[F]])(implicit F: Monad[F]): CompositeResolution[F] =
    new CompositeResolution[F](resolutions)

  /**
    * Constructs a [[CompositeResolution]] instance.
    *
    * @param resolutions underlying resolutions
    */
  final def apply[F[_]](resolutions: Resolution[F]*)(implicit F: Monad[F]): CompositeResolution[F] =
    new CompositeResolution[F](resolutions.toList)
}
