package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes, _}
import akka.http.scaladsl.server.Directives.{complete, fileUpload, parameter, pathPrefix, reject, _}
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
import ch.epfl.bluebrain.nexus.kg.resources.binary.Binary.{BinaryDescription, _}
import ch.epfl.bluebrain.nexus.kg.resources.binary.BinaryStore
import ch.epfl.bluebrain.nexus.kg.resources.binary.BinaryStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class BinaryRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    store: BinaryStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends Schemed(resources, binarySchemaUri, "binaries", acls, caller) {

  private val metadataRanges: Seq[MediaRange] = List(`application/json`, `application/ld+json`)

  private val simultaneousParamsRejection: AkkaRejection =
    validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously.")

  private type RejectionOrBinary = Either[AkkaRejection, Option[(BinaryAttributes, AkkaOut)]]

  override def createWithId(id: AbsoluteIri): Route =
    (put & parameter('rev.as[Long].?) & pathPrefix(IdSegment) & pathEndOrSingleSlash) { (rev, id) =>
      (projectNotDeprecated & hasPermission(resourceWrite) & identity) { implicit ident =>
        fileUpload("file") {
          case (metadata, byteSource) =>
            val description = BinaryDescription(metadata.fileName, metadata.contentType.value)
            trace(s"createBinaries") {
              val resId = Id(wrapped.ref, id)
              complete(resources.createBinaryWithId(resId, rev, description, byteSource).value.runToFuture)
            }
        }
      }
    }

  override def create: Route =
    (post & pathEndOrSingleSlash & projectNotDeprecated & hasPermission(resourceWrite) & identity) { implicit ident =>
      fileUpload("file") {
        case (metadata, byteSource) =>
          val description = BinaryDescription(metadata.fileName, metadata.contentType.value)
          trace(s"createBinaries") {
            complete(resources.createBinary(wrapped.ref, wrapped.base, description, byteSource).value.runToFuture)
          }
      }
    }

  override def update(id: AbsoluteIri): Route = reject()

  override def getResource(id: AbsoluteIri): Route =
    optionalHeaderValueByType[Accept](()) {
      case Some(h) if h.mediaRanges == metadataRanges => super.getResource(id)
      case _                                          => getBinary(id)
    }

  private def getBinary(id: AbsoluteIri): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & pathEndOrSingleSlash & hasPermission(resourceRead)) {
      (revOpt, tagOpt) =>
        val result = (revOpt, tagOpt) match {
          case (None, None) =>
            resources.fetchBinary(Id(wrapped.ref, id)).toEitherRun
          case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrBinary)
          case (Some(rev), _) =>
            resources.fetchBinary(Id(wrapped.ref, id), rev).toEitherRun
          case (_, Some(tag)) =>
            resources.fetchBinary(Id(wrapped.ref, id), tag).toEitherRun
        }
        trace(s"getBinaries") {
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

  private implicit class OptionTaskAttachmentSyntax(resource: OptionT[Task, (BinaryAttributes, AkkaOut)]) {

    def toEitherRun: Future[RejectionOrBinary] =
      resource.value.map[RejectionOrBinary](Right.apply).runToFuture
  }

  private def filenameHeader(info: BinaryAttributes) = {
    val filename = urlEncodeOrElse(info.filename)("attachment")
    RawHeader("Content-Disposition", s"attachment; filename*= UTF-8''$filename")
  }

  private def contentType(info: BinaryAttributes) =
    ContentType.parse(info.mediaType).getOrElse(`application/octet-stream`)
}
