package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.future._
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.iriResolution
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Ref, Resource, Resources}
import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}

/**
  * Resolution for a given project
  *
  * @param cache the distributed cache
  * @param staticResolution the static resolutions
  * @tparam F the monadic effect type
  */
abstract class ProjectResolution[F[_]: Monad](cache: DistributedCache[F], staticResolution: Resolution[F]) {

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

      private val resolution = cache.resolvers(ref).map { res =>
        val result = res.map {
          case _: StaticResolver       => staticResolution
          case r: InProjectResolver    => InProjectResolution[F](r.ref, resources)
          case r: InAccountResolver    => InAccountResolution[F](r.accountRef, resources, cache)
          case _: CrossProjectResolver => CrossProjectResolution[F](resources, res)
        }
        CompositeResolution(result)
      }

      def resolve(ref: Ref): F[Option[Resource]] =
        resolution.flatMap(_.resolve(ref))

      def resolveAll(ref: Ref): F[List[Resource]] =
        resolution.flatMap(_.resolveAll(ref))
    }

}
object ProjectResolution {

  /**
    * @param cache the distributed cache
    * @return a new [[ProjectResolution]] for the effect type [[Future]]
    */
  def future(cache: DistributedCache[Future])(implicit ec: ExecutionContext): ProjectResolution[Future] =
    new ProjectResolution(cache, StaticResolution[Future](iriResolution)) {}

  /**
    * @param cache the distributed cache
    * @return a new [[ProjectResolution]] for the effect type [[Task]]
    */
  def task(cache: DistributedCache[Task]): ProjectResolution[Task] =
    new ProjectResolution(cache, StaticResolution[Task](iriResolution)) {}

}
