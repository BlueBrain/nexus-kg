package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class FileRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "files", acls, caller, viewCache) {

  def routes: Route = {
    val fileRefOpt = Some(fileRef)
    create(fileRef) ~ list(fileRefOpt) ~
      pathPrefix(IdSegment) { id =>
        concat(
          update(id, fileRefOpt),
          create(id, fileRef),
          tag(id, fileRefOpt),
          deprecate(id, fileRefOpt),
          fetch(id, fileRefOpt),
          tags(id, fileRefOpt)
        )
      }
  }

  override def create(id: AbsoluteIri, schema: Ref): Route =
    (put & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) {
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = FileDescription(metadata.fileName, metadata.contentType.value)
          trace("createFile") {
            val resId = Id(project.ref, id)
            complete(resources.createFile(resId, description, byteSource).value.runWithStatus(Created))
          }
      }
    }

  override def create(schema: Ref): Route =
    (post & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) {
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = FileDescription(metadata.fileName, metadata.contentType.value)
          trace("createFile") {
            complete(
              resources.createFile(project.ref, project.base, description, byteSource).value.runWithStatus(Created))
          }
      }
    }

  override def update(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (put & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) { rev =>
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = FileDescription(metadata.fileName, metadata.contentType.value)
          trace("updateFile") {
            val resId = Id(project.ref, id)
            complete(resources.updateFile(resId, rev, description, byteSource).value.runWithStatus(OK))
          }
      }
    }
}
