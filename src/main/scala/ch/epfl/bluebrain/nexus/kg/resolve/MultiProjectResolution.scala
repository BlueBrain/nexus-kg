package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.ProjectCache
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri

/**
  * Common resolution logic for resolvers that look through several projects.
  *
  * @param resources    the resources operations
  * @param projects     the set of projects to traverse
  * @param types        the set of resource types to filter against
  * @param identities   the resolver identities
  * @param projectCache the project cache
  * @param acls         the ACLs for all identities in all projects
  * @tparam F the resolution effect type
  */
class MultiProjectResolution[F[_]](resources: Resources[F],
                                   projects: F[Set[ProjectRef]],
                                   types: Set[Iri.AbsoluteIri],
                                   identities: List[Identity],
                                   projectCache: ProjectCache[F],
                                   acls: AccessControlLists)(implicit F: Monad[F])
    extends Resolution[F] {

  private val read = Set(Permission.unsafe("resources/read"))

  override def resolve(ref: Ref): F[Option[Resource]] = {
    val sequence = projects.flatMap(_.map(p => checkPermsAndResolve(ref, p)).toList.sequence)
    sequence.map(_.collectFirst { case Some(r) => r })
  }

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.isEmpty || b.exists(a.contains)

  private def checkPermsAndResolve(ref: Ref, project: ProjectRef): F[Option[Resource]] =
    hasPermission(project).flatMap[Option[Resource]] {
      case false =>
        F.pure(None)
      case true =>
        InProjectResolution(project, resources).resolve(ref).map {
          case v @ Some(resource) if containsAny(resource.types, types) => v
          case _                                                        => None
        }
    }

  private def hasPermission(projectRef: ProjectRef): F[Boolean] =
    projectCache.getLabel(projectRef).map {
      case None        => false
      case Some(label) => acls.exists(identities.toSet, label, read)
    }
}

object MultiProjectResolution {

  /**
    * Builds a [[MultiProjectResolution]] instance.
    */
  def apply[F[_]: Monad](resources: Resources[F],
                         projects: F[Set[ProjectRef]],
                         types: Set[Iri.AbsoluteIri],
                         identities: List[Identity],
                         projectCache: ProjectCache[F],
                         acls: AccessControlLists): MultiProjectResolution[F] =
    new MultiProjectResolution(resources, projects, types, identities, projectCache, acls)
}
