package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.data.OptionT
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri

/**
  * Common resolution logic for resolvers that look through several projects.
  *
  * @param resources   the resources operations
  * @param projects    the set of projects to traverse
  * @param types       the set of resource types to filter against
  * @param identities  the resolver identities
  * @param adminClient the admin client
  * @param cache       the distributed cache
  * @tparam F the resolution effect type
  */
class MultiProjectResolution[F[_]](
    resources: Resources[F],
    projects: F[Set[ProjectRef]],
    types: Set[Iri.AbsoluteIri],
    identities: List[Identity],
    adminClient: AdminClient[F],
    cache: DistributedCache[F])(implicit F: Monad[F], serviceAccountToken: Option[AuthToken])
    extends Resolution[F] {

  private val resourceRead = Permissions(Permission("resources/read"), Permission("resources/manage"))

  override def resolve(ref: Ref): F[Option[Resource]] = projects.flatMap { set =>
    collectFirstM(set.toList) { project =>
      hasPermission(project).flatMap {
        case None => F.pure(None)
        case _    => resolveInProject(ref, project)
      }
    }
  }

  override def resolveAll(ref: Ref): F[List[Resource]] = projects.flatMap { set =>
    val traversal =
      set.toList.map { project =>
        hasPermission(project).flatMap[List[Resource]] {
          case None => F.pure(Nil)
          case _    => resolveInProject(ref, project).map(_.toList)
        }
      }
    traversal.flatSequence
  }

  /**
    * Collects the first non-empty element returned by the effectful function, or None if there aren't any.
    * Short-circuiting inspired by the [[cats.Foldable#existsM]] implementation.
    */
  private def collectFirstM[A](list: List[A])(f: A => F[Option[Resource]]): F[Option[Resource]] = {
    F.tailRecM(list) {
      case head :: tail =>
        f(head).map {
          case v @ Some(_) => Right(v)
          case None        => Left(tail)
        }
      case Nil => F.pure(Right(None))
    }
  }

  private def containsAny[A](a: Set[A], b: Set[A]): Boolean = b.isEmpty || b.exists(a.contains)

  private def projectToLabel(project: ProjectRef): F[Option[ProjectLabel]] =
    (for {
      proj <- OptionT(cache.project(project))
      projectLabel = proj.label
      accountRef <- OptionT(cache.accountRef(project))
      account    <- OptionT(cache.account(accountRef))
      accountLabel = account.label

    } yield ProjectLabel(accountLabel, projectLabel)).value

  private def hasPermission(project: ProjectRef): F[Option[FullAccessControl]] = projectToLabel(project).flatMap {
    case None => F.pure(None)
    case Some(label) =>
      adminClient
        .getProjectAcls(label.account, label.value, parents = true, self = false)
        .map { faclOpt =>
          faclOpt.flatMap { facl =>
            facl.acl.find(acl => (acl.permissions & resourceRead).nonEmpty && identities.contains(acl.identity))
          }
        }
  }

  private def resolveInProject(ref: Ref, project: ProjectRef): F[Option[Resource]] = {
    InProjectResolution(project, resources).resolve(ref).map {
      case v @ Some(resource) if containsAny(resource.types, types) => v
      case _                                                        => None
    }
  }

}

object MultiProjectResolution {

  /**
    * Builds a [[MultiProjectResolution]] instance.
    */
  def apply[F[_]: Monad](
      resources: Resources[F],
      projects: F[Set[ProjectRef]],
      types: Set[Iri.AbsoluteIri],
      identities: List[Identity],
      adminClient: AdminClient[F],
      cache: DistributedCache[F])(implicit serviceAccountToken: Option[AuthToken]): MultiProjectResolution[F] =
    new MultiProjectResolution(resources, projects, types, identities, adminClient, cache)
}
