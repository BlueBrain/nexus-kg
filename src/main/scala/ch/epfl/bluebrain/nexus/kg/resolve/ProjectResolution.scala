package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.future._
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.iriResolution
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Ref, Resource, Resources}
import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}

/**
  * Resolution for a given project
  *
  * @param projects         the project operations
  * @param staticResolution the static resolutions
  * @tparam F the monadic effect type
  */
abstract class ProjectResolution[F[_]: Monad](projects: Projects[F], staticResolution: Resolution[F]) {

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

      private val resolution = projects.resolvers(ref).map { res =>
        val result = res.map {
          case r: InProjectResolver    => InProjectResolution[F](r.ref, resources)
          case _: CrossProjectResolver => CrossProjectResolution[F](resources, res)
          case _                       => ??? // TODO: other kinds of resolver
        }
        CompositeResolution(staticResolution :: result)
      }

      def resolve(ref: Ref): F[Option[Resource]] =
        resolution.flatMap(_.resolve(ref))

      def resolveAll(ref: Ref): F[List[Resource]] =
        resolution.flatMap(_.resolveAll(ref))
    }

}
object ProjectResolution {

  /**
    * @param projects         the project operations
    * @return a new [[ProjectResolution]] for the effect type [[Future]]
    */
  def future(projects: Projects[Future])(implicit ec: ExecutionContext): ProjectResolution[Future] =
    new ProjectResolution(projects, StaticResolution[Future](iriResolution)) {}

  /**
    * @param projects         the project operations
    * @return a new [[ProjectResolution]] for the effect type [[Task]]
    */
  def task(projects: Projects[Task]): ProjectResolution[Task] =
    new ProjectResolution(projects, StaticResolution[Task](iriResolution)) {}

}
