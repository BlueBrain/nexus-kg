package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.UnacceptedResponseContentType
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.storage.{AkkaSource, Storage}
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class FileRoutes private[routes] (files: Files[Task], resources: Resources[Task], tags: Tags[Task])(
    implicit system: ActorSystem,
    acls: AccessControlLists,
    caller: Caller,
    project: Project,
    viewCache: ViewCache[Task],
    storageCache: StorageCache[Task],
    indexers: Clients[Task],
    config: AppConfig) {

  import indexers._

  /**
    * Routes for files. Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/files/{org}/{project}. E.g.: v1/files/myorg/myproject </li>
    *   <li> {prefix}/resources/{org}/{project}/{fileSchemaUri}. E.g.: v1/resources/myorg/myproject/file </li>
    * </ul>
    */
  def routes: Route =
    concat(
      // Create file when id is not provided in the Uri (POST)
      (post & projectNotDeprecated & pathEndOrSingleSlash & storage) { storage =>
        hasPermission(storage.writePermission).apply {
          fileUpload("file") {
            case (metadata, byteSource) =>
              val description = FileDescription(metadata.fileName, metadata.contentType)
              trace("createFile") {
                val created = files.create(storage, description, byteSource)
                complete(created.value.runWithStatus(Created))
              }
          } ~ entity(as[Json]) { source =>
            trace("createLink") {
              val created = files.createLink(storage, source)
              complete(created.value.runWithStatus(Created))
            }
          }
        }
      },
      // List files
      (get & paginated & searchParams(fixedSchema = fileSchemaUri) & pathEndOrSingleSlash & hasPermission(read) & extractUri) {
        (pagination, params, uri) =>
          trace("listFile") {
            implicit val u = uri
            val listed     = viewCache.getDefaultElasticSearch(project.ref).flatMap(files.list(_, params, pagination))
            complete(listed.runWithStatus(OK))
          }
      },
      // Consume the file id segment
      pathPrefix(IdSegment) { id =>
        routes(id)
      }
    )

  /**
    * Routes for files when the id is specified.
    * Those routes should get triggered after the following segments have been consumed:
    * <ul>
    *   <li> {prefix}/files/{org}/{project}/{id}. E.g.: v1/files/myorg/myproject/myfile </li>
    *   <li> {prefix}/resources/{org}/{project}/{fileSchemaUri}/{id}. E.g.: v1/resources/myorg/myproject/file/myfile</li>
    * </ul>
    */
  def routes(id: AbsoluteIri): Route =
    concat(
      // Create or update a file (depending on rev query parameter)
      (put & projectNotDeprecated & pathEndOrSingleSlash & storage) { storage =>
        val resId = Id(project.ref, id)
        hasPermission(storage.writePermission).apply {
          fileUpload("file") {
            case (metadata, byteSource) =>
              val description = FileDescription(metadata.fileName, metadata.contentType)
              parameter('rev.as[Long].?) {
                case None =>
                  trace("createFile") {
                    complete(files.create(resId, storage, description, byteSource).value.runWithStatus(Created))
                  }
                case Some(rev) =>
                  trace("updateFile") {
                    complete(files.update(resId, storage, rev, description, byteSource).value.runWithStatus(OK))
                  }
              }
          } ~ entity(as[Json]) { source =>
            parameter('rev.as[Long].?) {
              case None =>
                trace("createLink") {
                  complete(files.createLink(resId, storage, source).value.runWithStatus(Created))
                }
              case Some(rev) =>
                trace("updateLink") {
                  complete(files.updateLink(resId, storage, rev, source).value.runWithStatus(OK))
                }
            }
          }
        }
      },
      // Deprecate file
      (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        trace("deprecateFile") {
          complete(files.deprecate(Id(project.ref, id), rev).value.runWithStatus(OK))
        }
      },
      // Fetch file
      (get & outputFormat(strict = true, Binary) & hasPermission(read) & pathEndOrSingleSlash) {
        case Binary                        => getFile(id)
        case format: NonBinaryOutputFormat => getResource(id)(format)
      },
      new TagRoutes(tags, fileRef, write).routes(id)
    )

  private def getResource(id: AbsoluteIri)(implicit format: NonBinaryOutputFormat) =
    hasPermission(read).apply {
      trace("getFileMetadata") {
        concat(
          (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
            completeWithFormat(resources.fetch(Id(project.ref, id), rev, fileRef).value.runWithStatus(OK))
          },
          (parameter('tag) & noParameter('rev)) { tag =>
            completeWithFormat(resources.fetch(Id(project.ref, id), tag, fileRef).value.runWithStatus(OK))
          },
          (noParameter('tag) & noParameter('rev)) {
            completeWithFormat(resources.fetch(Id(project.ref, id), fileRef).value.runWithStatus(OK))
          }
        )
      }
    }

  private def getFile(id: AbsoluteIri): Route =
    trace("getFile") {
      concat(
        (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
          completeFile(files.fetch(Id(project.ref, id), rev).value.runToFuture)
        },
        (parameter('tag) & noParameter('rev)) { tag =>
          completeFile(files.fetch(Id(project.ref, id), tag).value.runToFuture)
        },
        (noParameter('tag) & noParameter('rev)) {
          completeFile(files.fetch(Id(project.ref, id)).value.runToFuture)
        }
      )
    }

  private def completeFile(f: Future[Either[Rejection, (Storage, FileAttributes, AkkaSource)]]): Route =
    onSuccess(f) {
      case Right((storage, info, source)) =>
        hasPermission(storage.readPermission).apply {
          val filename = urlEncodeOrElse(info.filename)("file")
          (respondWithHeaders(RawHeader("Content-Disposition", s"attachment; filename*=UTF-8''$filename")) & encodeResponse) {
            headerValueByType[Accept](()) { accept =>
              if (accept.mediaRanges.exists(_.matches(info.mediaType.mediaType)))
                complete(HttpEntity(info.mediaType, info.bytes, source))
              else
                failWith(
                  UnacceptedResponseContentType(
                    s"File Media Type '${info.mediaType}' does not match the Accept header value '${accept.mediaRanges
                      .mkString(", ")}'"))
            }
          }
        }
      case Left(err) => complete(err)
    }
}
