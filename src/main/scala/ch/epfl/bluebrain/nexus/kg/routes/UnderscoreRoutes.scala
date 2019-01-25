package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class UnderscoreRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    cache: Caches[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "resources", acls, caller, cache.view) {

  private implicit val viewCache: ViewCache[Task] = cache.view

  def routes: Route =
    create(unconstrainedRef) ~ list(None) ~
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

  private case class ResourceType(routes: CommonRoutes, schema: Option[Ref])

  private def fetchType(id: AbsoluteIri): Future[ResourceType] =
    resources
      .fetch(Id(project.ref, id), None)
      .map { res =>
        if (res.types(nxv.View.value)) ResourceType(new ViewRoutes(resources, acls, caller), Some(viewRef))
        else if (res.types(nxv.File.value)) ResourceType(new FileRoutes(resources, acls, caller), Some(fileRef))
        else if (res.types(nxv.Resolver.value))
          ResourceType(new ResolverRoutes(resources, acls, caller), Some(resolverRef))
        else if (res.types(nxv.Schema.value)) ResourceType(new SchemaRoutes(resources, acls, caller), Some(shaclRef))
        else ResourceType(UnderscoreRoutes.this, None)
      }
      .value
      .runNotFound(id.ref)

  private def update(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case ResourceType(rt, schema) => rt.update(id, schema)
  }

  private def tag(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case ResourceType(rt, schema) => rt.tag(id, schema)
  }

  private def tags(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case ResourceType(rt, schema) => rt.tags(id, schema)
  }

  private def deprecate(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case ResourceType(rt, schema) => rt.deprecate(id, schema)
  }

  private def fetch(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case ResourceType(rt, schema) => rt.fetch(id, schema)
  }
}
