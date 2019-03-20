package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.Caches._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator
import ch.epfl.bluebrain.nexus.kg.cache.{Caches, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.UnderscoreRoutes.ResourceType
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

final class UnderscoreRoutes private[routes] (resources: Resources[Task],
                                              acls: AccessControlLists,
                                              caller: Caller,
                                              projectViewCoordinator: ProjectViewCoordinator[Task])(
    implicit project: Project,
    cache: Caches[Task],
    indexers: Clients[Task],
    config: AppConfig)
    extends CommonRoutes(resources, "resources", acls, caller) {

  private implicit val viewCache: ViewCache[Task] = cache.view

  def routes: Route =
    create(unconstrainedRef) ~ list ~
      pathPrefix(IdSegment) { id =>
        concat(
          create(id, unconstrainedRef),
          update(id),
          tag(id),
          deprecate(id),
          fetch(id),
          tags(id)
        )
      }

  private def fetchType(id: AbsoluteIri): Future[Either[Rejection, ResourceType]] =
    resources
      .fetch(Id(project.ref, id))
      .map { res =>
        if (res.types(nxv.View.value))
          ResourceType(new ViewRoutes(resources, acls, caller, projectViewCoordinator), viewRef)
        else if (res.types(nxv.File.value)) ResourceType(new FileRoutes(resources, acls, caller), fileRef)
        else if (res.types(nxv.Resolver.value)) ResourceType(new ResolverRoutes(resources, acls, caller), resolverRef)
        else if (res.types(nxv.Schema.value)) ResourceType(new SchemaRoutes(resources, acls, caller), shaclRef)
        else ResourceType(UnderscoreRoutes.this, res.schema)
      }
      .value
      .runToFuture

  private def update(id: AbsoluteIri): Route = execute(id, rt => rt.routes.update(id, rt.schema))

  private def tag(id: AbsoluteIri): Route = execute(id, rt => rt.routes.tag(id, rt.schema))

  private def tags(id: AbsoluteIri): Route = execute(id, rt => rt.routes.tags(id, rt.schema))

  private def deprecate(id: AbsoluteIri): Route = execute(id, rt => rt.routes.deprecate(id, rt.schema))

  private def fetch(id: AbsoluteIri): Route = execute(id, rt => rt.routes.fetch(id, rt.schema))

  private def execute(id: AbsoluteIri, operation: (ResourceType) => Route): Route =
    onSuccess(fetchType(id)) {
      case Right(rt)       => operation(rt)
      case Left(rejection) => complete(rejection)
    }
}

object UnderscoreRoutes {
  private final case class ResourceType(routes: CommonRoutes, schema: Ref)
}
