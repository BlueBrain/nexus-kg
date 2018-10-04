package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryDescription
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.{Attachment, AttachmentStore}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import com.github.ghik.silencer.silent
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

/**
  * Routes for a specific resource and schema type defined by ''prefix'' abd ''schema'' operations
  *
  * @param resources the resources operations
  * @param schema    the schema to which the routes match
  * @param prefix    the prefix to which the routes match. E.g.: Resources, Schemas, Resolvers, Views
  * @param acls      the ACLs for all identities in all projects
  * @param caller    the [[Caller]] with all it's identities
  */
private[routes] class ResourceRoutes(resources: Resources[Task],
                                     schema: AbsoluteIri,
                                     prefix: String,
                                     acls: FullAccessControlList,
                                     caller: Caller)(implicit wrapped: LabeledProject,
                                                     cache: DistributedCache[Task],
                                                     indexers: Clients[Task],
                                                     store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                                     config: AppConfig) {

  import indexers._
  implicit val acl = acls
  implicit val c   = caller

  private val suffixTracing = prefix.capitalize

  def transformCreate(j: Json): Json = j

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def transformUpdate(@silent id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] =
    EitherT.rightT(j)

  def transformGet(resource: ResourceV): Task[ResourceV] =
    Task.pure(resource)

  implicit def additional: AdditionalValidation[Task] =
    AdditionalValidation.pass

  def routes: Route = {
    create ~ list ~ pathPrefix(IdSegment) { id =>
      // format: off
      update(id) ~ createWithId(id) ~ tag(id) ~ deprecate(id) ~ addAttachment(id) ~ removeAttachment(id) ~ getResource(id) ~ getResourceAttachment(id)
      // format: on
    }
  }

  def list: Route =
    (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
      (deprecated, pagination) =>
        trace(s"list$suffixTracing") {
          complete(cache.views(wrapped.ref).flatMap(v => resources.list(v, deprecated, schema, pagination)).runAsync)
        }
    }

  def create: Route =
    (projectNotDeprecated & post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
      (identity & hasPermission(resourceCreate)) { implicit ident =>
        trace(s"create$suffixTracing") {
          val created = resources.create(wrapped.ref, wrapped.base, Ref(schema), transformCreate(source))
          complete(Created -> created.value.runAsync)
        }
      }
    }

  def createWithId(id: AbsoluteIri): Route =
    (put & entity(as[Json]) & projectNotDeprecated & pathEndOrSingleSlash) { source =>
      (identity & hasPermission(resourceCreate)) { implicit ident =>
        trace(s"create$suffixTracing") {
          complete(
            Created -> resources
              .createWithId(Id(wrapped.ref, id), Ref(schema), transformCreate(source))
              .value
              .runAsync)
        }
      }
    }

  def update(id: AbsoluteIri): Route =
    (put & entity(as[Json]) & projectNotDeprecated & parameter('rev.as[Long]) & pathEndOrSingleSlash) { (json, rev) =>
      (identity & hasPermission(resourceWrite)) { implicit ident =>
        trace(s"update$suffixTracing") {
          complete(
            transformUpdate(id, json)
              .flatMap(transformed => resources.update(Id(wrapped.ref, id), rev, Some(Ref(schema)), transformed))
              .value
              .runAsync)
        }
      }
    }

  def tag(id: AbsoluteIri): Route =
    (pathPrefix("tags") & projectNotDeprecated & put & entity(as[Json]) & parameter('rev.as[Long]) & pathEndOrSingleSlash) {
      (json, rev) =>
        (identity & hasPermission(resourceWrite)) { implicit ident =>
          trace(s"addTag$suffixTracing") {
            val tagged = resources.tag(Id(wrapped.ref, id), rev, Some(Ref(schema)), json.addContext(tagCtxUri))
            complete(Created -> tagged.value.runAsync)
          }
        }
    }

  def deprecate(id: AbsoluteIri): Route =
    (delete & projectNotDeprecated & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
      (identity & hasPermission(resourceWrite)) { implicit ident =>
        trace(s"deprecate$suffixTracing") {
          complete(resources.deprecate(Id(wrapped.ref, id), rev, Some(Ref(schema))).value.runAsync)
        }
      }
    }

  def addAttachment(id: AbsoluteIri): Route =
    (pathPrefix("attachments" / Segment) & projectNotDeprecated & put & parameter('rev.as[Long]) & pathEndOrSingleSlash) {
      (filename, rev) =>
        (identity & hasPermission(resourceWrite)) { implicit ident =>
          fileUpload("file") {
            case (metadata, byteSource) =>
              val description = BinaryDescription(filename, metadata.contentType.value)
              trace(s"addAttachment$suffixTracing") {
                complete(
                  resources
                    .attach(Id(wrapped.ref, id), rev, Some(Ref(schema)), description, byteSource)
                    .value
                    .runAsync)
              }
          }
        }
    }

  def removeAttachment(id: AbsoluteIri): Route =
    (pathPrefix("attachments" / Segment) & projectNotDeprecated & delete & parameter('rev.as[Long]) & pathEndOrSingleSlash) {
      (filename, rev) =>
        (identity & hasPermission(resourceWrite)) { implicit ident =>
          trace(s"removeAttachment$suffixTracing") {
            complete(resources.unattach(Id(wrapped.ref, id), rev, Some(Ref(schema)), filename).value.runAsync)
          }
        }
    }

  def getResource(id: AbsoluteIri): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & pathEndOrSingleSlash) { (revOpt, tagOpt) =>
      (identity & hasPermission(resourceRead)) { _ =>
        trace(s"get$suffixTracing") {
          (revOpt, tagOpt) match {
            case (None, None) =>
              complete(resources.fetch(Id(wrapped.ref, id), Some(Ref(schema))).materializeRun(Ref(id)))
            case (Some(_), Some(_)) =>
              reject(simultaneousParamsRejection)
            case (Some(rev), _) =>
              complete(resources.fetch(Id(wrapped.ref, id), rev, Some(Ref(schema))).materializeRun(Ref(id)))
            case (_, Some(tag)) =>
              complete(resources.fetch(Id(wrapped.ref, id), tag, Some(Ref(schema))).materializeRun(Ref(id)))
          }
        }
      }
    }

  def getResourceAttachment(id: AbsoluteIri): Route =
    (parameter('rev.as[Long].?) & parameter('tag.?)) { (revOpt, tagOpt) =>
      (pathPrefix("attachments" / Segment) & get & pathEndOrSingleSlash) { filename =>
        (identity & hasPermission(resourceRead)) { _ =>
          val result = (revOpt, tagOpt) match {
            case (None, None) =>
              resources.fetchAttachment(Id(wrapped.ref, id), Some(Ref(schema)), filename).toEitherRun
            case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrAttachment)
            case (Some(rev), _) =>
              resources.fetchAttachment(Id(wrapped.ref, id), rev, Some(Ref(schema)), filename).toEitherRun
            case (_, Some(tag)) =>
              resources.fetchAttachment(Id(wrapped.ref, id), tag, Some(Ref(schema)), filename).toEitherRun
          }
          trace(s"getAttachment$suffixTracing") {
            onSuccess(result) {
              case Left(rej) => reject(rej)
              case Right(Some((info, source))) =>
                respondWithHeaders(filenameHeader(info)) {
                  encodeResponse {
                    complete(HttpEntity(contentType(info), info.byteSize, source))
                  }
                }
              case _ =>
                complete(StatusCodes.NotFound)
            }
          }
        }
      }
    }

  private val simultaneousParamsRejection: AkkaRejection =
    validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously.")

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun(ref: => Ref): Future[Either[Rejection, ResourceV]] =
      (for {
        res          <- resource.toRight(NotFound(ref): Rejection)
        materialized <- resources.materializeWithMeta(res)
        transformed  <- EitherT.right[Rejection](transformGet(materialized))
      } yield transformed).value.runAsync
  }

  private type RejectionOrAttachment = Either[AkkaRejection, Option[(Attachment.BinaryAttributes, AkkaOut)]]
  private implicit class OptionTaskAttachmentSyntax(resource: OptionT[Task, (Attachment.BinaryAttributes, AkkaOut)]) {
    def toEitherRun: Future[RejectionOrAttachment] =
      resource.value.map[RejectionOrAttachment](Right.apply).runAsync
  }

  private def filenameHeader(info: Attachment.BinaryAttributes) = {
    val filename = urlEncodeOrElse(info.filename)("attachment")
    RawHeader("Content-Disposition", s"attachment; filename*= UTF-8''$filename")
  }

  private def contentType(info: Attachment.BinaryAttributes) =
    ContentType.parse(info.mediaType).getOrElse(`application/octet-stream`)

  private[routes] val resourceRead =
    Permissions(Permission(s"$prefix/read"), Permission(s"$prefix/manage"))
  private[routes] val resourceWrite =
    Permissions(Permission(s"$prefix/write"), Permission(s"$prefix/manage"))
  private[routes] val resourceCreate =
    Permissions(Permission(s"$prefix/create"), Permission(s"$prefix/manage"))

}

object ResourceRoutes {

  private[routes] def apply(resources: Resources[Task], schema: AbsoluteIri, prefixSegment: String)(
      implicit cache: DistributedCache[Task],
      token: Option[AuthToken],
      indexers: Clients[Task],
      aclsOps: AclsOps,
      store: AttachmentStore[Task, AkkaIn, AkkaOut],
      config: AppConfig): Route = {

    import indexers._

    // consumes the segment {prefixSegment}/{account}/{project}
    (pathPrefix(prefixSegment) & project) { implicit wrapped =>
      //Fetches the ACLs for the current project
      (acls & caller) { (acl, c) =>
        new ResourceRoutes(resources, schema, prefixSegment, acl, c).routes
      }
    }
  }
}
