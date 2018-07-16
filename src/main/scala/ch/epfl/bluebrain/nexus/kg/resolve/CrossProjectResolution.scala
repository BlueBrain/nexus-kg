package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.CrossProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Implementation that fetches the [[CrossProjectResolver]] from the cache and handles the resolution process
  * of references to resources within all the projects defined in the resolver.
  *
  * TODO: Missing ACLs verification
  *
  * @param resources the resources operations
  * @param resolvers the cross project resolvers
  * @tparam F the resolution effect type
  */
class CrossProjectResolution[F[_]] private (resources: Resources[F], resolvers: List[CrossProjectResolver])(
    implicit F: Monad[F])
    extends Resolution[F] {

  private val projects = resolvers.flatMap(r => r.projects.map(_ -> r.resourceTypes)).distinct

  override def resolve(ref: Ref): F[Option[Resource]] =
    projects.foldLeft[F[Option[Resource]]](F.pure(None)) {
      case (acc, (proj, types)) =>
        acc.flatMap {
          case v @ Some(_) => F.pure(v)
          case _ =>
            InProjectResolution(proj, resources).resolve(ref).map {
              case resourceOpt @ Some(resource) if containsAny(resource.types, types) => resourceOpt
              case _                                                                  => None
            }
        }
    }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    projects
      .foldLeft(F.pure(List.empty[Resource])) {
        case (acc, (p, types)) =>
          InProjectResolution(p, resources).resolve(ref).flatMap {
            case Some(resource) if containsAny(resource.types, types) => acc.map(resource :: _)
            case _                                                    => acc
          }
      }
      .map(_.reverse)

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.exists(a.contains)

}

object CrossProjectResolution {

  /**
    * Constructs a [[CrossProjectResolution]] instance
    *
    * @param resources the resources operations
    * @param resolvers the cross project resolvers
    */
  def apply[F[_]: Monad](resources: Resources[F], resolvers: List[Resolver]): CrossProjectResolution[F] =
    new CrossProjectResolution(resources, resolvers.sortBy(_.priority).collect {
      case r: CrossProjectResolver if !r.deprecated => r
    })
}
