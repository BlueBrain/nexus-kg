package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri

/**
  * Common resolution logic for resolvers that look through several projects.
  *
  * TODO: Missing ACLs verification
  *
  * @param resources the resources operations
  * @param projects  a list of project references and the respective resource types that will
  *                  be traversed, in the provided order, by the resolution logic.
  * @tparam F the resolution effect type
  */
class MultiProjectResolution[F[_]](resources: Resources[F], projects: F[List[(ProjectRef, Set[Iri.AbsoluteIri])]])(
    implicit F: Monad[F])
    extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = projects.flatMap {
    _.foldLeft[F[Option[Resource]]](F.pure(None)) {
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
  }

  override def resolveAll(ref: Ref): F[List[Resource]] = projects.flatMap {
    _.foldLeft(F.pure(List.empty[Resource])) {
      case (acc, (p, types)) =>
        InProjectResolution(p, resources).resolve(ref).flatMap {
          case Some(resource) if containsAny(resource.types, types) => acc.map(resource :: _)
          case _                                                    => acc
        }
    }.map(_.reverse)
  }

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.exists(a.contains)

}

object MultiProjectResolution {
  def apply[F[_]: Monad](resources: Resources[F],
                         projects: F[List[(ProjectRef, Set[Iri.AbsoluteIri])]]): MultiProjectResolution[F] =
    new MultiProjectResolution(resources, projects)
}
