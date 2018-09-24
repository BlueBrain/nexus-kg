package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.data.OptionT
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri

/**
  * Common resolution logic for resolvers that look through several projects.
  *
  * @param resources  the resources operations
  * @param projects   the set of projects to traverse
  * @param types      the set of resource types to filter against
  * @param identities the resolver identities
  * @param cache      the distributed cache
  * @param acls       the ACLs for all identities in all projects
  * @tparam F the resolution effect type
  */
class MultiProjectResolution[F[_]](resources: Resources[F],
                                   projects: F[Set[ProjectRef]],
                                   types: Set[Iri.AbsoluteIri],
                                   identities: List[Identity],
                                   cache: DistributedCache[F],
                                   acls: FullAccessControlList)(implicit F: Monad[F])
    extends Resolution[F] {

  private val resourceRead = Permissions(Permission("resources/read"), Permission("resources/manage"))

  override def resolve(ref: Ref): F[Option[Resource]] = {
    val sequence = projects.flatMap(_.map(p => checkPermsAndResolve(ref, p)).toList.sequence)
    sequence.map(_.collectFirst { case Some(r) => r })
  }

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.isEmpty || b.exists(a.contains)

  private def projectToLabel(project: ProjectRef): F[Option[ProjectLabel]] = {
    val result = for {
      // format: off
      proj          <- OptionT(cache.project(project))
      projectLabel   = proj.label
      accountRef    <- OptionT(cache.accountRef(project))
      account       <- OptionT(cache.account(accountRef))
      accountLabel   = account.label
    } yield ProjectLabel(accountLabel, projectLabel)
    // format: on
    result.value
  }

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

  private def hasPermission(project: ProjectRef): F[Boolean] =
    projectToLabel(project).map {
      case None        => false
      case Some(label) => acls.exists(identities.toSet, label, resourceRead)
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
                         cache: DistributedCache[F],
                         acls: FullAccessControlList): MultiProjectResolution[F] =
    new MultiProjectResolution(resources, projects, types, identities, cache, acls)
}
