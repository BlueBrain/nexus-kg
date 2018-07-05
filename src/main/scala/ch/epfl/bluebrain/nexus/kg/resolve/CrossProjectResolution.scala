package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.CrossProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Implementation that fetches the [[CrossProjectResolver]] from the cache and handles the resolution process
  * of references to resources within all the projects defined in the resolver.
  *
  * TODO: Missing ACLs verification
  *
  * @param project  the resolution scope
  * @tparam F the resolution effect type
  */
class CrossProjectResolution[F[_]: Repo](project: ProjectRef)(implicit F: Monad[F], projects: Projects[F])
    extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] =
    projects.resolvers(project).crossProjectSorted.flatMap {
      _.foldLeft[F[Option[Resource]]](F.pure(None)) {
        case (acc, (proj, types)) =>
          acc.flatMap {
            case v @ Some(_) => F.pure(v)
            case _ =>
              InProjectResolution(proj).resolve(ref).map {
                case resourceOpt @ Some(resource) if containsAny(resource.types, types) => resourceOpt
                case _                                                                  => None
              }
          }
      }
    }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    projects.resolvers(project).crossProjectSorted.flatMap {
      _.foldLeft(F.pure(List.empty[Resource])) {
        case (acc, (p, types)) =>
          InProjectResolution(p).resolve(ref).flatMap {
            case Some(resource) if containsAny(resource.types, types) => acc.map(resource :: _)
            case _                                                    => acc
          }
      }.map(_.reverse)
    }

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.exists(a.contains)

  private implicit class ResolverSetSyntax(values: F[Set[Resolver]]) {
    def crossProjectSorted: F[List[(ProjectRef, Set[AbsoluteIri])]] =
      values
        .map(_.collect {
          case r: CrossProjectResolver if !r.deprecated => r
        }.toList.sortBy(_.priority))
        .map(_.flatMap(r => r.projects.map(_ -> r.resourceTypes)).distinct)

  }

}
