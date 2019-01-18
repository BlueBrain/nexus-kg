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
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
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
    extends CommonRoutes(resources, "underscore", acls, caller, cache.view) {

  private implicit val viewCache: ViewCache[Task] = cache.view

  private val fileRoutes = new FileRoutes(resources, acls, caller)
  private val viewRoutes = new ViewRoutes(resources, acls, caller)

  def routes: Route = {
    create(resourceRef) ~ list(None) ~
      pathPrefix(IdSegment) { id =>
        concat(
          update(id),
          create(id),
          tag(id),
          deprecate(id),
          fetch(id)
        )
      }
  }

  private sealed trait ResourceType extends Product with Serializable
  private case object ViewType      extends ResourceType
  private case object FileType      extends ResourceType
  private case object OtherType     extends ResourceType

  private def fetchType(id: AbsoluteIri): Future[Either[Rejection, ResourceType]] = {
    resources.fetch(Id(project.ref, id), None).value.runToFuture.map {
      case None => Left(NotFound(Ref(id)): Rejection)
      case Some(res) =>
        if (res.types(nxv.View.value)) Right(ViewType)
        else if (res.types(nxv.File.value)) Right(FileType)
        else Right(OtherType)
    }
  }

  private def create(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case Right(FileType)  => fileRoutes.create(id, fileRef)
    case Right(ViewType)  => viewRoutes.create(id, viewRef)
    case Right(OtherType) => super.create(id, resourceRef)
    case Left(rejection)  => complete(rejection)
  }

  private def update(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case Right(FileType)  => fileRoutes.update(id, Some(fileRef))
    case Right(ViewType)  => viewRoutes.update(id, Some(viewRef))
    case Right(OtherType) => super.update(id, Some(resourceRef))
    case Left(rejection)  => complete(rejection)
  }

  private def tag(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case Right(FileType)  => fileRoutes.tag(id, Some(fileRef))
    case Right(ViewType)  => viewRoutes.tag(id, Some(viewRef))
    case Right(OtherType) => super.tag(id, Some(resourceRef))
    case Left(rejection)  => complete(rejection)
  }

  private def deprecate(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case Right(FileType)  => fileRoutes.deprecate(id, Some(fileRef))
    case Right(ViewType)  => viewRoutes.deprecate(id, Some(viewRef))
    case Right(OtherType) => super.deprecate(id, Some(resourceRef))
    case Left(rejection)  => complete(rejection)
  }

  private def fetch(id: AbsoluteIri): Route = onSuccess(fetchType(id)) {
    case Right(FileType)  => fileRoutes.fetch(id, Some(fileRef))
    case Right(ViewType)  => viewRoutes.fetch(id, Some(viewRef))
    case Right(OtherType) => super.fetch(id, Some(resourceRef))
    case Left(rejection)  => complete(rejection)
  }
}
