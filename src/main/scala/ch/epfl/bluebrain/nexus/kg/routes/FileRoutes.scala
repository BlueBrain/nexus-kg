package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
import cats.data.OptionT
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes.`application/ld+json`
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.{hasPermission, identity}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class FileRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends Schemed(resources, fileSchemaUri, "files", acls, caller) {

  private val metadataRanges: Seq[MediaRange] = List(`application/json`, `application/ld+json`)

  private type RejectionOrFile = Either[AkkaRejection, Option[(FileAttributes, AkkaOut)]]

  override def createWithId(id: AbsoluteIri): Route =
    (put & pathPrefix(IdSegment) & pathEndOrSingleSlash) { id =>
      (projectNotDeprecated & hasPermission(resourceCreate) & identity) { implicit ident =>
        fileUpload("file") {
          case (metadata, byteSource) =>
            val description = FileDescription(metadata.fileName, metadata.contentType.value)
            trace("createFiles") {
              val resId = Id(wrapped.ref, id)
              complete(resources.createFileWithId(resId, description, byteSource).value.runToFuture)
            }
        }
      }
    }

  override def create: Route =
    (post & pathEndOrSingleSlash & projectNotDeprecated & hasPermission(resourceCreate) & identity) { implicit ident =>
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = FileDescription(metadata.fileName, metadata.contentType.value)
          trace("createFiles") {
            complete(resources.createFile(wrapped.ref, wrapped.base, description, byteSource).value.runToFuture)
          }
      }
    }

  override def update(id: AbsoluteIri): Route =
    (put & parameter('rev.as[Long]) & pathPrefix(IdSegment) & pathEndOrSingleSlash) { (rev, id) =>
      (projectNotDeprecated & hasPermission(resourceWrite) & identity) { implicit ident =>
        fileUpload("file") {
          case (metadata, byteSource) =>
            val description = FileDescription(metadata.fileName, metadata.contentType.value)
            trace("updateFiles") {
              val resId = Id(wrapped.ref, id)
              complete(resources.updateFile(resId, rev, description, byteSource).value.runToFuture)
            }
        }
      }
    }

  override def getResource(id: AbsoluteIri): Route =
    optionalHeaderValueByType[Accept](()) {
      case Some(h) if h.mediaRanges == metadataRanges => super.getResource(id)
      case _                                          => getFile(id)
    }

  private def getFile(id: AbsoluteIri): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & pathEndOrSingleSlash & hasPermission(resourceRead)) {
      (revOpt, tagOpt) =>
        val result = (revOpt, tagOpt) match {
          case (None, None) =>
            resources.fetchFile(Id(wrapped.ref, id)).toEitherRun
          case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrFile)
          case (Some(rev), _) =>
            resources.fetchFile(Id(wrapped.ref, id), rev).toEitherRun
          case (_, Some(tag)) =>
            resources.fetchFile(Id(wrapped.ref, id), tag).toEitherRun
        }
        trace("getFiles") {
          onSuccess(result) {
            case Left(rej) =>
              reject(rej)
            case Right(Some((info, source))) =>
              (respondWithHeaders(filenameHeader(info)) & encodeResponse) {
                complete(HttpEntity(contentType(info), info.byteSize, source))
              }
            case _ =>
              complete(StatusCodes.NotFound)
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
