package ch.epfl.bluebrain.nexus.kg.routes

import java.net.URLEncoder

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives.{fileUpload, parameter, pathPrefix, _}
import akka.http.scaladsl.server.Route
import cats.data.OptionT
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Permission, Permissions}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.{hasPermission, _}
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives.{projectReference, _}
import ch.epfl.bluebrain.nexus.kg.marshallers.ResourceJsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.kg.resolve.{InProjectResolution, Resolution}
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryDescription
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.{Attachment, AttachmentStore}
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, _}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import scala.util.Try

class ResourceRoutes(implicit repo: Repo[Task],
                     adminClient: AdminClient[Future],
                     iamClient: IamClient[Future],
                     store: AttachmentStore[Task, AkkaIn, AkkaOut]) {

  def routes: Route =
    token(implicit optToken => resources)

  private def resources(implicit token: Option[AuthToken]): Route =
    (pathPrefix("resources") & project) { implicit proj =>
      projectReference() { implicit projRef =>
        // create resource with implicit or generated id
        (post & aliasOrCuriePath & entity(as[Json])) { (schema, source) =>
          (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
            complete(create[Task](proj.ref, proj.base, Ref(schema), source).value.runAsync)
          }
        } ~
          pathPrefix(aliasOrCurie / aliasOrCurie) { (schema, id) =>
            // create resource with explicit id
            (put & entity(as[Json]) & pathEndOrSingleSlash) { source =>
              (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
                complete(create[Task](Id(proj.ref, id), Ref(schema), source).value.runAsync)
              }
            } ~
              parameter('rev.as[Long]) { rev =>
                // update a resource
                (put & entity(as[Json]) & pathEndOrSingleSlash) { source =>
                  (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
                    complete(update[Task](Id(proj.ref, id), rev, Some(Ref(schema)), source).value.runAsync)
                  }
                } ~
                  // tag a resource
                  (put & entity(as[Json]) & pathPrefix("tags") & pathEndOrSingleSlash) { json =>
                    (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
                      complete(tag[Task](Id(proj.ref, id), rev, Some(Ref(schema)), json).value.runAsync)
                    }
                  } ~
                  // deprecate a resource
                  (delete & pathEndOrSingleSlash) {
                    (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
                      complete(deprecate[Task](Id(proj.ref, id), rev, Some(Ref(schema))).value.runAsync)
                    }
                  }
                // remove a resource attachment
                (path("attachments" ~ Segment) & pathEndOrSingleSlash) { filename =>
                  (delete & pathEndOrSingleSlash) {
                    (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
                      complete(unattach[Task](Id(proj.ref, id), rev, Some(Ref(schema)), filename).value.runAsync)
                    }
                  } ~
                    // add a resource attachment
                    (put & pathEndOrSingleSlash) {
                      (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
                        fileUpload("file") {
                          case (metadata, byteSource) =>
                            val description = BinaryDescription(filename, metadata.contentType.value)
                            complete(attach[Task, AkkaIn](Id(proj.ref, id),
                                                          rev,
                                                          Some(Ref(schema)),
                                                          description,
                                                          byteSource).value.runAsync)
                        }
                      }
                    }
                }
              } ~
              (parameter('rev.as[Long].?) & parameter('tag.?)) { (revOpt, tagOpt) =>
                // get a resource
                (get & pathEndOrSingleSlash) {
                  (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
                    (revOpt, tagOpt) match {
                      case (None, None) =>
                        complete(fetch[Task](Id(proj.ref, id), Some(Ref(schema))).materializeRun)
                      case (Some(_), Some(_)) =>
                        reject(
                          validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously."))
                      case (Some(rev), _) =>
                        complete(fetch[Task](Id(proj.ref, id), rev, Some(Ref(schema))).materializeRun)
                      case (_, Some(tag)) =>
                        complete(fetch[Task](Id(proj.ref, id), tag, Some(Ref(schema))).materializeRun)
                    }
                  }
                } ~
                  (path("attachments" ~ Segment) & pathEndOrSingleSlash) { filename =>
                    // get a resource attachment
                    (get & callerIdentity & hasPermission(resourceRead) & pathEndOrSingleSlash) { implicit ident =>
                      val result = (revOpt, tagOpt) match {
                        case (None, None) =>
                          fetchAttachment[Task, AkkaOut](Id(proj.ref, id), Some(Ref(schema)), filename).value.runAsync
                        //TODO: We will deal with this case later on
                        case (Some(_), Some(_)) => Future.failed(new RuntimeException())
                        case (Some(rev), _) =>
                          fetchAttachment[Task, AkkaOut](Id(proj.ref, id), rev, Some(Ref(schema)), filename).value.runAsync
                        case (_, Some(tag)) =>
                          fetchAttachment[Task, AkkaOut](Id(proj.ref, id), tag, Some(Ref(schema)), filename).value.runAsync
                      }
                      onSuccess(result) {
                        case Some((info, source)) =>
                          respondWithHeaders(filenameHeader(info)) {
                            complete(HttpEntity(contentType(info), info.contentSize.value, source))
                          }
                        case None =>
                          complete(StatusCodes.NotFound)
                      }
                    }
                  }
              }
          }
      }
    }

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun(implicit r: Resolution[Task]): Future[Option[ResourceV]] =
      resource.flatMap(materializeWithMeta[Task](_).toOption).value.runAsync
  }

  private def filenameHeader(info: Attachment.BinaryAttributes) = {
    val filename = encodedFilenameOrElse(info, "attachment")
    RawHeader("Content-Disposition", s"attachment; filename*= UTF-8''$filename")
  }

  private def contentType(info: Attachment.BinaryAttributes) =
    ContentType.parse(info.mediaType).getOrElse(`application/octet-stream`)

  private def encodedFilenameOrElse(info: Attachment.BinaryAttributes, value: => String): String =
    Try(URLEncoder.encode(info.filename, "UTF-8")).getOrElse(value)

  private implicit class ProjectSyntax(proj: Project) {
    def ref: ProjectRef = ProjectRef(proj.uuid)
  }

  private implicit def projectToResolution(implicit proj: Project): Resolution[Task] =
    InProjectResolution[Task](proj.ref)

  private implicit def resourceEncoder: Encoder[Resource] = Encoder.encodeJson.contramap { res =>
    json(res.id.value, res.metadata(_.iri) ++ res.typeGraph, res.value.contextValue)
  }

  private implicit def resourceVEncoder: Encoder[ResourceV] = Encoder.encodeJson.contramap { res =>
    json(res.id.value, res.value.graph, res.value.ctx)
  }

  private def json(id: AbsoluteIri, graph: Graph, ctx: Json): Json = {
    val primaryNode     = Some(IriNode(id))
    val mergedCtx: Json = ctx mergeContext resourceCtx
    val jsonResult      = graph.asJson(mergedCtx, primaryNode).getOrElse(graph.asJson)
    jsonResult deepMerge Json.obj("@context" -> ctx).addContext(resourceCtxUri)
  }

}

object ResourceRoutes {
  final def apply()(implicit repo: Repo[Task],
                    adminClient: AdminClient[Future],
                    iamClient: IamClient[Future],
                    store: AttachmentStore[Task, AkkaIn, AkkaOut]): ResourceRoutes = new ResourceRoutes()

  private[routes] val resourceRead   = Permissions(Permission("resources/read"), Permission("resources/manage"))
  private[routes] val resourceWrite  = Permissions(Permission("resources/write"), Permission("resources/manage"))
  private[routes] val resourceCreate = Permissions(Permission("resources/create"), Permission("resources/manage"))
}
