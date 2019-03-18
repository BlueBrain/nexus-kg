package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.kg.async.{AclsCache, ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.iriResolution
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources._
import monix.eval.Task

/**
  * Resolution for a given project
  *
  * @param resolverCache the resolver cache
  * @param projectCache the project cache
  * @param staticResolution the static resolutions
  * @param aclCache the acl cache
  * @tparam F the monadic effect type
  */
class ProjectResolution[F[_]](resolverCache: ResolverCache[F],
                              projectCache: ProjectCache[F],
                              staticResolution: Resolution[F],
                              aclCache: AclsCache[F])(implicit F: Monad[F]) {

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

      def resolverResolution(resolver: Resolver): F[Resolution[F]] =
        resolver match {
          case r: InProjectResolver => F.pure(InProjectResolution[F](r.ref, resources))
          case r @ CrossProjectResolver(_, `Set[ProjectRef]`(projects), _, _, _, _, _, _) =>
            aclCache.list.map(
              MultiProjectResolution(resources, F.pure(projects), r.resourceTypes, r.identities, projectCache, _))
        }

      private val resolution = resolverCache.get(ref).flatMap {
        _.filterNot(_.deprecated)
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
    * @param resolverCache the resolver cache
    * @param projectCache  the project cache
    * @param aclCache      the acl cache
    * @return a new [[ProjectResolution]] for the effect type [[Task]]
    */
  def task(resolverCache: ResolverCache[Task],
           projectCache: ProjectCache[Task],
           aclCache: AclsCache[Task]): ProjectResolution[Task] =
    new ProjectResolution(resolverCache, projectCache, StaticResolution[Task](iriResolution), aclCache)

}
