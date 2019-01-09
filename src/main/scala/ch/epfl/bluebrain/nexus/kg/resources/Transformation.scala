package ch.epfl.bluebrain.nexus.kg.resources

import cats.Monad
import cats.syntax.applicative._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resolverCtxUri, viewCtxUri}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._

abstract class Transformation[F[_], V] {

  /**
    * Performs transformations on a resource with metadata
    *
    * @param resource the resource
    * @return the transformed resource wrapped on the effect type ''F''
    */
  def apply(resource: ResourceV)(implicit config: AppConfig,
                                 wrapped: LabeledProject,
                                 cache: DistributedCache[F],
                                 enc: GraphEncoder[V]): F[ResourceV]
}

object Transformation {

  /**
    * Attempts to convert the resource into a [[View]], adds the metadata and attempts to convert the
    * project UUIDs into project labels. If the conversion doesn't work, returns the resource unchanged.
    *
    * @tparam F the monadic effect type
    * @return a new resource with view transformations
    */
  final def view[F[_]: Monad]: Transformation[F, View] =
    new Transformation[F, View] {
      def apply(resource: ResourceV)(implicit config: AppConfig,
                                     wrapped: LabeledProject,
                                     cache: DistributedCache[F],
                                     enc: GraphEncoder[View]): F[ResourceV] =
        View(resource) match {
          case Right(r) =>
            val metadata = resource.metadata
            val resValueF =
              r.labeled.getOrElse(r).map(r => resource.value.map(r, _.removeKeys("@context").addContext(viewCtxUri)))
            resValueF.map(v => resource.map(_ => v.copy(graph = v.graph ++ Graph(metadata))))
          case _ => resource.pure
        }
    }

  /**
    * Attempts to convert the resource into a [[Resolver]], adds the metadata and attempts to convert the
    * project UUIDs into project labels. If the conversion doesn't work, returns the resource unchanged.
    *
    * @tparam F the monadic effect type
    * @return a new resource with resolver transformations
    */
  final def resolver[F[_]: Monad]: Transformation[F, Resolver] =
    new Transformation[F, Resolver] {

      def apply(resource: ResourceV)(implicit config: AppConfig,
                                     wrapped: LabeledProject,
                                     cache: DistributedCache[F],
                                     enc: GraphEncoder[Resolver]): F[ResourceV] =
        Resolver(resource) match {
          case Some(r) =>
            val metadata = resource.metadata
            val resValueF =
              r.labeled
                .getOrElse(r)
                .map(r => resource.value.map(r, _.removeKeys("@context").addContext(resolverCtxUri)))
            resValueF.map(v => resource.map(_ => v.copy(graph = v.graph ++ Graph(metadata))))
          case _ => resource.pure
        }
    }
}
