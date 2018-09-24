package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.iam.client.types.FullAccessControlList
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.iriResolution
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources._
import monix.eval.Task

/**
  * Resolution for a given project
  *
  * @param cache the distributed cache
  * @param staticResolution the static resolutions
  * @param fetchAcls the function to fetch ACLs
  * @tparam F the monadic effect type
  */
class ProjectResolution[F[_]](cache: DistributedCache[F],
                              staticResolution: Resolution[F],
                              fetchAcls: => F[FullAccessControlList])(implicit F: Monad[F]) {

  /**
    * Looks up the collection of defined resolvers for the argument project
    * and generates an aggregated [[Resolution]] out of them.
    *
    * @param ref       the project reference
    * @param resources the resource operations
    * @return a new [[Resolution]] which is composed by all the resolutions generated from
    *         the resolvers found for the given ''projectRef''
    */
  def apply(ref: ProjectRef)(resources: Resources[F]): Resolution[F] =
    new Resolution[F] {

      def resolverResolution(r: Resolver): F[Resolution[F]] =
        r match {
          case r: InProjectResolver => F.pure(InProjectResolution[F](r.ref, resources))
          case r: InAccountResolver =>
            val projects = cache.projects(r.accountRef)
            fetchAcls.map(MultiProjectResolution(resources, projects, r.resourceTypes, r.identities, cache, _))
          case r: CrossProjectResolver =>
            fetchAcls.map(
              MultiProjectResolution(resources, F.pure(r.projects), r.resourceTypes, r.identities, cache, _))
        }

      private val resolution = cache.resolvers(ref).flatMap {
        _.filterNot(_.deprecated).toList
          .sortBy(_.priority)
          .map(resolverResolution)
          .sequence
          .map(list => CompositeResolution(staticResolution :: list))
      }

      def resolve(ref: Ref): F[Option[Resource]] =
        resolution.flatMap(_.resolve(ref))
    }

}

object ProjectResolution {

  /**
    * @param cache the distributed cache
    * @return a new [[ProjectResolution]] for the effect type [[Task]]
    */
  def task(cache: DistributedCache[Task], aclsOps: AclsOps): ProjectResolution[Task] =
    new ProjectResolution(cache, StaticResolution[Task](iriResolution), aclsOps.fetch)

}
