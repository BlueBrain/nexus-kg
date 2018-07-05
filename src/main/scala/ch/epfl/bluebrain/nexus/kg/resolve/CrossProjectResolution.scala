package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.CrossProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Implementation that fetches the [[CrossProjectResolver]] from the cache and handles the resolution process
  * of references to resources within all the projects defined in the resolver.
  *
  * TODO: Missing ACLs verification
  * TODO: Missing resourceType check
  *
  * @param project  the resolution scope
  * @tparam F the resolution effect type
  */
class CrossProjectResolution[F[_]: Repo](project: ProjectRef)(implicit F: Monad[F], projects: Projects[F])
    extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] =
    projects.resolvers(project).crossProjectSorted.flatMap { projects =>
      projects.foldLeft[F[Option[Resource]]](F.pure(None)) { (acc, p) =>
        acc.flatMap {
          case (v @ Some(_)) => F.pure(v)
          case (None)        => InProjectResolution(p).resolve(ref)
        }
      }
    }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    projects.resolvers(project).crossProjectSorted.flatMap { projects =>
      projects
        .foldLeft(F.pure(List.empty[Resource])) { (acc, p) =>
          InProjectResolution(p).resolve(ref).flatMap {
            case Some(res) => acc.map(res :: _)
            case None      => acc
          }
        }
        .map(_.reverse)
    }

  private implicit class ResolverSetSyntax(values: F[Set[Resolver]]) {
    def crossProjectSorted: F[List[ProjectRef]] =
      values
        .map(_.collect {
          case r: CrossProjectResolver if !r.deprecated => r
        }.toList.sortBy(_.priority))
        .map(_.flatMap(_.projects).distinct)

  }

}
