package ch.epfl.bluebrain.nexus.kg.resources

import cats.MonadError
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.async.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resolverCtxUri, viewCtxUri}
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}

abstract class Transformation[F[_], V] {

  /**
    * Performs transformations on a resource with metadata
    *
    * @param resource the resource
    * @return the transformed resource wrapped on the effect type ''F''
    */
  def apply(
      resource: ResourceV)(implicit config: AppConfig, project: Project, projectCache: ProjectCache[F]): F[ResourceV]
}

object Transformation {

  /**
    * Attempts to convert the resource into a [[View]], adds the metadata and attempts to convert the
    * project UUIDs into project labels. If the conversion doesn't work, returns the resource unchanged.
    *
    * @tparam F the monadic effect type
    * @return a new resource with view transformations
    */
  final def view[F[_]](implicit F: MonadError[F, Throwable]): Transformation[F, View] =
    new Transformation[F, View] {
      def apply(resource: ResourceV)(implicit config: AppConfig,
                                     project: Project,
                                     projectCache: ProjectCache[F]): F[ResourceV] =
        View(resource) match {
          case Right(r) =>
            val metadata = resource.metadata
            r.labeled.getOrElse(r).flatMap { view =>
              resource.value.map(view, _.removeKeys("@context").addContext(viewCtxUri)) match {
                case None => F.raiseError(InternalError("Could not convert view to Json"))
                case Some(value) =>
                  F.pure(resource.map(_ =>
                    value.copy(graph = RootedGraph(value.graph.rootNode, value.graph ++ Graph(metadata)))))
              }
            }
          case _ => F.pure(resource)
        }
    }

  /**
    * Attempts to convert the resource into a [[Resolver]], adds the metadata and attempts to convert the
    * project UUIDs into project labels. If the conversion doesn't work, returns the resource unchanged.
    *
    * @tparam F the monadic effect type
    * @return a new resource with resolver transformations
    */
  final def resolver[F[_]](implicit F: MonadError[F, Throwable]): Transformation[F, Resolver] =
    new Transformation[F, Resolver] {

      def apply(resource: ResourceV)(implicit config: AppConfig,
                                     project: Project,
                                     projectCache: ProjectCache[F]): F[ResourceV] =
        Resolver(resource) match {
          case Some(r) =>
            val metadata = resource.metadata
            r.labeled
              .getOrElse(r)
              .flatMap { resolver =>
                resource.value.map(resolver, _.removeKeys("@context").addContext(resolverCtxUri)) match {
                  case None => F.raiseError(InternalError("Could not convert resolver to Json"))
                  case Some(value) =>
                    F.pure(resource.map(_ =>
                      value.copy(graph = RootedGraph(value.graph.rootNode, value.graph ++ Graph(metadata)))))
                }
              }
          case _ => F.pure(resource)
        }
    }
}
