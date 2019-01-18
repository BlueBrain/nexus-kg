package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
import cats.data.OptionT
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes.`application/ld+json`
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
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import ch.epfl.bluebrain.nexus.kg.resources.syntax._

import scala.concurrent.Future

class FileRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "files", acls, caller, viewCache) {

  private val metadataRanges: Seq[MediaRange] = List(`application/json`, `application/ld+json`)
  private type RejectionOrFile = Either[AkkaRejection, Option[(FileAttributes, AkkaOut)]]

  def routes: Route = {
    val fileRefOpt = Some(fileRef)
    create(fileRef) ~ list(fileRefOpt) ~
      pathPrefix(IdSegment) { id =>
        concat(
          update(id, fileRefOpt),
          create(id, fileRef),
          tag(id, fileRefOpt),
          deprecate(id, fileRefOpt),
          fetch(id, fileRefOpt)
        )
      }
  }

  override def create(id: AbsoluteIri, schema: Ref): Route =
    pathPrefix(IdSegment) { id =>
      (put & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
        fileUpload("file") {
          case (metadata, byteSource) =>
            val description = FileDescription(metadata.fileName, metadata.contentType.value)
            trace("createFiles") {
              val resId = Id(project.ref, id)
              complete(resources.createFile(resId, description, byteSource).value.runToFuture)
            }
        }
      }
    }

  override def create(schema: Ref): Route =
    (post & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = FileDescription(metadata.fileName, metadata.contentType.value)
          trace("createFiles") {
            complete(resources.createFile(project.ref, project.base, description, byteSource).value.runToFuture)
          }
      }
    }

  override def update(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    pathPrefix(IdSegment) { id =>
      (put & parameter('rev.as[Long]) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
        rev =>
          fileUpload("file") {
            case (metadata, byteSource) =>
              val description = FileDescription(metadata.fileName, metadata.contentType.value)
              trace("updateFiles") {
                val resId = Id(project.ref, id)
                complete(resources.updateFile(resId, rev, description, byteSource).value.runToFuture)
              }
          }
      }
    }

  override def fetch(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    optionalHeaderValueByType[Accept](()) {
      case Some(h) if h.mediaRanges == metadataRanges => super.fetch(id, schemaOpt)
      case _                                          => getFile(id)
    }

  private def getFile(id: AbsoluteIri): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & hasPermission(readPermission) & pathEndOrSingleSlash) {
      (revOpt, tagOpt) =>
        val result = (revOpt, tagOpt) match {
          case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrFile)
          case (None, None)       => resources.fetchFile(Id(project.ref, id)).toEitherRun
          case (Some(rev), _)     => resources.fetchFile(Id(project.ref, id), rev).toEitherRun
          case (_, Some(tag))     => resources.fetchFile(Id(project.ref, id), tag).toEitherRun
        }
        trace("getFiles") {
          onSuccess(result) {
            case Left(rej) => reject(rej)
            case Right(Some((info, source))) =>
              (respondWithHeaders(filenameHeader(info)) & encodeResponse) {
                complete(HttpEntity(contentType(info), info.byteSize, source))
              }
            case _ => complete(StatusCodes.NotFound)
          }
        }
    }

  private implicit class OptionTaskFileSyntax(resource: OptionT[Task, (FileAttributes, AkkaOut)]) {

    def toEitherRun: Future[RejectionOrFile] =
      resource.value.map[RejectionOrFile](Right.apply).runToFuture
  }

  private def filenameHeader(info: FileAttributes) = {
    val filename = urlEncodeOrElse(info.filename)("file")
    RawHeader("Content-Disposition", s"attachment; filename*= UTF-8''$filename")
  }

  private def contentType(info: FileAttributes) =
    ContentType.parse(info.mediaType).getOrElse(`application/octet-stream`)
}
