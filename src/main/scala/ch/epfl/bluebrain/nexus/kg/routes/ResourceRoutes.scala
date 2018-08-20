package ch.epfl.bluebrain.nexus.kg.routes

import java.util.UUID

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticDecoder
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.withTaskUnmarshaller
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.DeprecatedId._
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.marshallers.{ExceptionHandling, RejectionHandling}
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.ElasticDecoders.resourceIdDecoder
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryDescription
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.{Attachment, AttachmentStore}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.service.kamon.directives.TracingDirectives
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

/**
  * Routes for resources operations
  *
  * @param resources the resources operations
  */
class ResourceRoutes(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                                 indexers: Clients[Task],
                                                 store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                                 config: AppConfig,
                                                 tracing: TracingDirectives) {

  private val (es, sparql) = (indexers.elastic, indexers.sparql)
  import indexers._
  import tracing._

  def routes: Route =
    handleRejections(RejectionHandling()) {
      handleExceptions(ExceptionHandling()) {
        token { implicit optToken =>
          pathPrefix(config.http.prefix) {
            res ~ schemas ~ resolvers ~ views ~ search ~ listings
          }
        }
      }
    }

  private def res(implicit token: Option[AuthToken]): Route =
    // consumes the segment resources/{account}/{project}
    (pathPrefix("resources") & project) { implicit wrapped =>
      // create resource with implicit or generated id
      (post & projectNotDeprecated & pathPrefix(IdSegment) & entity(as[Json]) & pathEndOrSingleSlash) {
        (schema, source) =>
          (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
            trace("createResource") {
              complete(
                Created -> resources.create(wrapped.ref, wrapped.project.base, Ref(schema), source).value.runAsync)
            }
          }
      } ~
        pathPrefix(IdSegment / IdSegment)((schema, id) => resources(schema, id))
    }

  private def transformView(source: Json): Json =
    transformView(source, UUID.randomUUID().toString.toLowerCase)

  private def transformView(source: Json, uuid: String): Json = {
    val transformed = source deepMerge Json.obj("uuid" -> Json.fromString(uuid)).addContext(viewCtxUri)
    transformed.hcursor.get[Json]("mapping") match {
      case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
      case _                      => transformed
    }
  }

  private def transformViewUpdate(resId: ResId)(source: Json): EitherT[Task, Rejection, Json] =
    resources
      .fetch(resId, Some(Latest(viewSchemaUri)))
      .toRight(NotFound(resId.ref): Rejection)
      .flatMap(r =>
        EitherT.fromEither(r.value.hcursor.get[String]("uuid").left.map(_ => UnexpectedState(resId.ref): Rejection)))
      .map(uuid => transformView(source, uuid))

  private def views(implicit token: Option[AuthToken]): Route =
    // consumes the segment views/{account}/{project}
    (pathPrefix("views") & project) { implicit wrapped =>
      // create view with implicit or generated id
      (projectNotDeprecated & post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
        (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
          trace("createView") {
            complete(
              Created -> resources
                .create(wrapped.ref, wrapped.base, Ref(viewSchemaUri), transformView(source))
                .value
                .runAsync)
          }
        }
      } ~ listings(viewSchemaUri) ~
        pathPrefix(IdSegment)(id =>
          resources(viewSchemaUri, id, transformView, transformViewUpdate(Id(wrapped.ref, id))))
    }

  private def schemas(implicit token: Option[AuthToken]): Route =
    // consumes the segment schemas/{account}/{project}
    (pathPrefix("schemas") & project) { implicit wrapped =>
      // create schema with implicit or generated id
      (post & projectNotDeprecated & entity(as[Json]) & pathEndOrSingleSlash) { source =>
        (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
          trace("createSchema") {
            complete(
              Created -> resources
                .create(wrapped.ref, wrapped.project.base, Ref(shaclSchemaUri), source)
                .value
                .runAsync)
          }
        }
      } ~ listings(shaclSchemaUri) ~
        pathPrefix(IdSegment)(id => resources(shaclSchemaUri, id))
    }

  private def resolvers(implicit token: Option[AuthToken]): Route =
    // consumes the segment resolvers/{account}/{project}
    (pathPrefix("resolvers") & project) { implicit wrapped =>
      // create resolver with implicit or generated id
      (projectNotDeprecated & post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
        callerIdentity.apply { implicit ident =>
          acls.apply { implicit acls =>
            hasPermissionInAcl(resourceCreate).apply {
              trace("createResolver") {
                complete(
                  Created -> resources
                    .create(wrapped.ref, wrapped.base, Ref(resolverSchemaUri), source.addContext(resolverCtxUri))
                    .value
                    .runAsync)
              }
            }
          }
        }
      } ~ listings(resolverSchemaUri) ~
        pathPrefix(IdSegment) { id =>
          acls.apply { implicit acls =>
            resources(resolverSchemaUri, id, _.addContext(resolverCtxUri), _.addContext(resolverCtxUri))
          }
        }
    }

  private def search(implicit token: Option[AuthToken]): Route =
    // consumes the segment views/{account}/{project}
    (pathPrefix("views") & project) { implicit wrapped =>
      (pathPrefix(IdSegment / "sparql") & post & entity(as[String]) & pathEndOrSingleSlash) { (id, query) =>
        (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
          val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
            views.find(_.id == id) match {
              case Some(v: SparqlView) => sparql.copy(namespace = v.name).queryRaw(urlEncode(query)).map(Right.apply)
              case _                   => Task.pure(Left(NotFound(Ref(id))))
            }
          }
          trace("searchSparql")(complete(result.runAsync))
        }
      } ~
        (pathPrefix(IdSegment / "_search") & post & entity(as[Json]) & paginated & extract(_.request.uri.query()) & pathEndOrSingleSlash) {
          (id, query, pagination, params) =>
            (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
              val result: Task[Either[Rejection, List[Json]]] = cache.views(wrapped.ref).flatMap { views =>
                views.find(_.id == id) match {
                  case Some(v: ElasticView) =>
                    val index = s"${config.elastic.indexPrefix}_${v.name}"
                    es.search[Json](query, Set(index), params)(pagination).map(qr => Right(qr.results.map(_.source)))
                  case _ =>
                    Task.pure(Left(NotFound(Ref(id))))
                }
              }
              trace("searchElastic")(complete(result.runAsync))
            }
        }
    }

  private def listings(schema: AbsoluteIri)(implicit wrapped: LabeledProject, token: Option[AuthToken]): Route = {
    (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
      (deprecated, pagination) =>
        schema match {
          case `resolverSchemaUri` =>
            trace("listResolvers") {
              val qr = filterDeprecated(cache.resolvers(wrapped.ref), deprecated).map { r =>
                toQueryResults(r.sortBy(_.priority))
              }
              complete(qr.runAsync)
            }

          case `viewSchemaUri` =>
            trace("listViews") {
              complete(filterDeprecated(cache.views(wrapped.ref), deprecated).map(toQueryResults).runAsync)
            }
          case _ =>
            trace("listResources") {
              complete(
                cache.views(wrapped.ref).flatMap(v => resources.list(v, deprecated, schema, pagination)).runAsync)
            }
        }
    }
  }

  private def listings(implicit token: Option[AuthToken]): Route =
    (pathPrefix("resources") & project) { implicit wrapped =>
      (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
        (deprecated, pagination) =>
          val results = cache.views(wrapped.ref).flatMap(v => resources.list(v, deprecated, pagination))
          trace("listResources") {
            complete(results.runAsync)
          }
      } ~ pathPrefix(IdSegment)(listings)
    }

  private def resources(schema: AbsoluteIri,
                        id: AbsoluteIri,
                        transformCreate: Json => Json = j => j,
                        transformUpdate: Json => EitherT[Task, Rejection, Json] = j => EitherT.rightT(j))(
      implicit wrapped: LabeledProject,
      token: Option[AuthToken],
      additional: AdditionalValidation[Task] = AdditionalValidation.pass): Route =
    (put & entity(as[Json]) & projectNotDeprecated & pathEndOrSingleSlash) { source =>
      parameter('rev.as[Long].?) {
        case Some(rev) =>
          // updates a resource
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            trace("updateResource") {
              complete(
                transformUpdate(source)
                  .flatMap(transformed => resources.update(Id(wrapped.ref, id), rev, Some(Ref(schema)), transformed))
                  .value
                  .runAsync)
            }
          }
        case None =>
          // create resource with explicit id
          (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
            trace("createResource") {
              complete(
                Created -> resources
                  .createWithId(Id(wrapped.ref, id), Ref(schema), transformCreate(source))
                  .value
                  .runAsync)
            }
          }
      }
    } ~
      // add tag to resource
      (pathPrefix("tags") & projectNotDeprecated) {
        (put & parameter('rev.as[Long]) & entity(as[Json]) & pathEndOrSingleSlash) { (rev, json) =>
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            trace("addTagResource") {
              complete(
                Created -> resources
                  .tag(Id(wrapped.ref, id), rev, Some(Ref(schema)), json.addContext(tagCtxUri))
                  .value
                  .runAsync)
            }
          }
        }
      } ~
      // deprecate a resource
      (delete & projectNotDeprecated & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
        (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
          trace("deprecateResource") {
            complete(resources.deprecate(Id(wrapped.ref, id), rev, Some(Ref(schema))).value.runAsync)
          }
        }
      } ~
      (pathPrefix("attachments" / Segment) & projectNotDeprecated) { filename =>
        // remove a resource attachment
        (delete & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            trace("removeAttachmentResource") {
              complete(resources.unattach(Id(wrapped.ref, id), rev, Some(Ref(schema)), filename).value.runAsync)
            }
          }
        } ~
          // add an attachment to resources
          (put & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
            (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
              fileUpload("file") {
                case (metadata, byteSource) =>
                  val description = BinaryDescription(filename, metadata.contentType.value)
                  trace("addAttachmentResource") {
                    complete(
                      resources
                        .attach(Id(wrapped.ref, id), rev, Some(Ref(schema)), description, byteSource)
                        .value
                        .runAsync)
                  }
              }
            }
          }
      } ~
      (parameter('rev.as[Long].?) & parameter('tag.?)) { (revOpt, tagOpt) =>
        // get a resource
        (get & pathEndOrSingleSlash) {
          (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
            trace("getResource") {
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
        } ~
          (pathPrefix("attachments" / Segment) & pathEndOrSingleSlash) { filename =>
            // get a resource attachment
            (get & callerIdentity & hasPermission(resourceRead)) { implicit ident =>
              val result = (revOpt, tagOpt) match {
                case (None, None) =>
                  resources.fetchAttachment(Id(wrapped.ref, id), Some(Ref(schema)), filename).toEitherRun
                case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrAttachment)
                case (Some(rev), _) =>
                  resources.fetchAttachment(Id(wrapped.ref, id), rev, Some(Ref(schema)), filename).toEitherRun
                case (_, Some(tag)) =>
                  resources.fetchAttachment(Id(wrapped.ref, id), tag, Some(Ref(schema)), filename).toEitherRun
              }
              trace("getResourceAttachment") {
                onSuccess(result) {
                  case Left(rej) => reject(rej)
                  case Right(Some((info, source))) =>
                    respondWithHeaders(filenameHeader(info)) {
                      complete(HttpEntity(contentType(info), info.contentSize.value, source))
                    }
                  case _ =>
                    complete(StatusCodes.NotFound)
                }
              }
            }
          }
      }

  private def filterDeprecated[A: DeprecatedId](set: Task[Set[A]], deprecated: Option[Boolean]): Task[List[A]] =
    set.map(s => deprecated.map(d => s.filter(_.deprecated == d)).getOrElse(s).toList)

  private implicit def qrsClient(implicit wrapped: LabeledProject,
                                 http: HttpConfig): HttpClient[Task, QueryResults[AbsoluteIri]] = {
    implicit val elasticDec = ElasticDecoder(resourceIdDecoder)
    withTaskUnmarshaller[QueryResults[AbsoluteIri]]
  }

  private val simultaneousParamsRejection: AkkaRejection =
    validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously.")

  private implicit def toEither[A](a: A): EitherT[Task, Rejection, A] =
    EitherT.rightT[Task, Rejection](a)

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun(ref: => Ref): Future[Either[Rejection, ResourceV]] =
      resource.toRight(NotFound(ref): Rejection).flatMap(resources.materializeWithMeta(_)).value.runAsync
  }

  private def toQueryResults[A](resolvers: List[A]): QueryResults[A] =
    UnscoredQueryResults(resolvers.size.toLong, resolvers.map(UnscoredQueryResult(_)))

  private type RejectionOrAttachment = Either[AkkaRejection, Option[(Attachment.BinaryAttributes, AkkaOut)]]
  private implicit class OptionTaskAttachmentSyntax(resource: OptionT[Task, (Attachment.BinaryAttributes, AkkaOut)]) {
    def toEitherRun: Future[RejectionOrAttachment] =
      resource.value.map[RejectionOrAttachment](Right.apply).runAsync
  }

  private implicit def resolverAclValidation(implicit acls: Option[FullAccessControlList],
                                             wrapped: LabeledProject): AdditionalValidation[Task] =
    AdditionalValidation.resolver(acls, wrapped.accountRef, cache.projectRef)

  private def filenameHeader(info: Attachment.BinaryAttributes) = {
    val filename = urlEncodeOrElse(info.filename)("attachment")
    RawHeader("Content-Disposition", s"attachment; filename*= UTF-8''$filename")
  }

  private def contentType(info: Attachment.BinaryAttributes) =
    ContentType.parse(info.mediaType).getOrElse(`application/octet-stream`)

  private implicit def toProject(implicit value: LabeledProject): Project           = value.project
  private implicit def toProjectLabel(implicit value: LabeledProject): ProjectLabel = value.label

}

object ResourceRoutes {

  /**
    * @param resources the resources operations
    * @return a new insrtance of a [[ResourceRoutes]]
    */
  final def apply(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                              indexers: Clients[Task],
                                              store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                              config: AppConfig): ResourceRoutes = {
    implicit val tracing = new TracingDirectives()
    new ResourceRoutes(resources)
  }

  private[routes] val resourceRead   = Permissions(Permission("resources/read"), Permission("resources/manage"))
  private[routes] val resourceWrite  = Permissions(Permission("resources/write"), Permission("resources/manage"))
  private[routes] val resourceCreate = Permissions(Permission("resources/create"), Permission("resources/manage"))
}
