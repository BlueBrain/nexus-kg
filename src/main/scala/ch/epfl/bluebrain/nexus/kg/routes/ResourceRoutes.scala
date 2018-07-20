package ch.epfl.bluebrain.nexus.kg.routes

import java.net.URLEncoder

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route, Rejection => AkkaRejection}
import cats.data.OptionT
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticDecoder
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.withTaskUnmarshaller
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Permission, Permissions}
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.ElasticDecoders.resourceIdDecoder
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryDescription
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.{Attachment, AttachmentStore}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path.FromString
import ch.epfl.bluebrain.nexus.service.http.UriOps._
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import scala.util.Try

/**
  * Routes for resources operations
  *
  * @param resources the resources operations
  */
class ResourceRoutes(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                                 indexers: Clients[Task],
                                                 store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                                 config: AppConfig) {

  private val (es, sparql) = (indexers.elastic, indexers.sparql)
  import indexers._

  def routes: Route =
    handleRejections(RejectionHandling.rejectionHandler()) {
      token { implicit optToken =>
        pathPrefix(config.http.prefix) {
          res ~ schemas ~ resolvers ~ search ~ listings
        }
      }
    }

  private def res(implicit token: Option[AuthToken]): Route =
    // consumes the segment resources/{account}/{project}
    (pathPrefix("resources") & project) { implicit wrapped =>
      // create resource with implicit or generated id
      (post & projectNotDeprecated & aliasOrCuriePath & entity(as[Json])) { (schema, source) =>
        (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
          complete(
            Created -> resources.create(wrapped.project.ref, wrapped.project.base, Ref(schema), source).value.runAsync)
        }
      } ~
        pathPrefix(aliasOrCurie / aliasOrCurie)((schema, id) => resources(schema, id))
    }

  private def schemas(implicit token: Option[AuthToken]): Route =
    // consumes the segment schemas/{account}/{project}
    (pathPrefix("schemas") & project) { implicit wrapped =>
      // create schema with implicit or generated id
      (post & projectNotDeprecated & entity(as[Json])) { source =>
        (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
          complete(
            Created -> resources
              .create(wrapped.project.ref, wrapped.project.base, Ref(shaclSchemaUri), source)
              .value
              .runAsync)
        }
      } ~
        pathPrefix(aliasOrCurie)(id => resources(shaclSchemaUri, id))
    }

  private def resolvers(implicit token: Option[AuthToken]): Route =
    // consumes the segment resolvers/{account}/{project}
    (pathPrefix("resolvers") & project) { implicit wrapped =>
      // create resolvers with implicit or generated id
      (projectNotDeprecated & post & entity(as[Json])) { source =>
        (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
          complete(
            Created -> resources
              .create(wrapped.project.ref,
                      wrapped.project.base,
                      Ref(crossResolverSchemaUri),
                      source.addContext(resolverCtxUri))
              .value
              .runAsync)
        }
      } ~
        // list resolvers with implicit or generated id
        (get & parameter('deprecated.as[Boolean].?) & pathEndOrSingleSlash) { deprecated =>
          (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
            val resolvers = cache.resolvers(wrapped.label).map { r =>
              val filtered = deprecated
                .map(d => r.filter(_.deprecated == d))
                .getOrElse(r)
                .toList
                .sortBy(_.priority)
              toQueryResults(filtered)
            }
            complete(resolvers.runAsync)
          }
        } ~
        pathPrefix(aliasOrCurie) { id =>
          resources(crossResolverSchemaUri,
                    id,
                    withAttachment = false,
                    withTag = false,
                    injectUri = Some(resolverCtxUri))
        }
    }

  private def search(implicit token: Option[AuthToken]): Route =
    // consumes the segment views/{account}/{project}
    (pathPrefix("views") & project) { implicit wrapped =>
      // search forwarded to the sparql endpoint of the default view
      (path("sparql") & post & entity(as[Json]) & pathEndOrSingleSlash) { query =>
        (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
          complete(sparql.copy(namespace = config.sparql.defaultIndex).queryRaw(query.noSpaces).runAsync)
        }
      } ~
        // search forwarded to the elastic endpoint of the default view
        (path("elastic") & post & entity(as[Json]) & paginated & extract(_.request.uri.query()) & pathEndOrSingleSlash) {
          (query, pagination, params) =>
            (callerIdentity & hasPermission(resourceRead)) { implicit ident =>
              val search = es.search[Json](query, Set(config.elastic.defaultIndex), params)(pagination)
              //TODO: Treat ES response accordingly
              complete(search.map(_.results.map(_.source)).runAsync)
            }
        }
    }

  private def listings(implicit token: Option[AuthToken]): Route =
    (pathPrefix("resources") & project) { implicit proj =>
      implicit val decoder = ElasticDecoder(
        resourceIdDecoder(
          url"${config.http.publicUri.append("resources" / proj.label.account / proj.label.value)}".value))
      implicit val cl = withTaskUnmarshaller[QueryResults[AbsoluteIri]]
      (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead)) {
        (deprecated, pagination) =>
          val results = cache.views(proj.project.ref).flatMap(v => resources.list(v, deprecated, pagination))
          complete(results.runAsync)
      } ~ (get & parameter('deprecated.as[Boolean].?) & paginated & aliasOrCuriePath & hasPermission(resourceRead)) {
        (deprecated, pagination, schema) =>
          val results = cache.views(proj.project.ref).flatMap(v => resources.list(v, deprecated, schema, pagination))
          complete(results.runAsync)
      }
    }

  private def resources(schema: AbsoluteIri,
                        id: AbsoluteIri,
                        withTag: Boolean = true,
                        withAttachment: Boolean = true,
                        injectUri: Option[AbsoluteIri] = None)(implicit
                                                               proj: Project,
                                                               projRef: ProjectLabel,
                                                               token: Option[AuthToken]): Route =
    (put & entity(as[Json]) & projectNotDeprecated & pathEndOrSingleSlash) { source =>
      parameter('rev.as[Long].?) {
        case Some(rev) =>
          // update a resource
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            complete(
              resources.update(Id(proj.ref, id), rev, Some(Ref(schema)), source.addContext(injectUri)).value.runAsync)
          }
        case None =>
          // create resource with explicit id
          (callerIdentity & hasPermission(resourceCreate)) { implicit ident =>
            complete(
              Created -> resources.create(Id(proj.ref, id), Ref(schema), source.addContext(injectUri)).value.runAsync)
          }
      }
    } ~
      // tag a resource
      (evalBool(withTag) & pathPrefix("tags") & projectNotDeprecated) {
        (put & parameter('rev.as[Long]) & entity(as[Json]) & pathEndOrSingleSlash) { (rev, json) =>
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            complete(
              Created -> resources
                .tag(Id(proj.ref, id), rev, Some(Ref(schema)), json.addContext(tagCtxUri))
                .value
                .runAsync)
          }
        }
      } ~
      // deprecate a resource
      (delete & projectNotDeprecated & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
        (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
          complete(resources.deprecate(Id(proj.ref, id), rev, Some(Ref(schema))).value.runAsync)
        }
      } ~
      (pathPrefix("attachments" / Segment) & evalBool(withAttachment) & projectNotDeprecated) { filename =>
        // remove a resource attachment
        (delete & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
          (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
            complete(resources.unattach(Id(proj.ref, id), rev, Some(Ref(schema)), filename).value.runAsync)
          }
        } ~
          // add a resource attachment
          (put & parameter('rev.as[Long]) & pathEndOrSingleSlash) { rev =>
            (callerIdentity & hasPermission(resourceWrite)) { implicit ident =>
              fileUpload("file") {
                case (metadata, byteSource) =>
                  val description = BinaryDescription(filename, metadata.contentType.value)
                  complete(
                    resources
                      .attach(Id(proj.ref, id), rev, Some(Ref(schema)), description, byteSource)
                      .value
                      .runAsync)
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
                complete(resources.fetch(Id(proj.ref, id), Some(Ref(schema))).materializeRun)
              case (Some(_), Some(_)) =>
                reject(simultaneousParamsRejection)
              case (Some(rev), _) =>
                complete(resources.fetch(Id(proj.ref, id), rev, Some(Ref(schema))).materializeRun)
              case (_, Some(tag)) =>
                complete(resources.fetch(Id(proj.ref, id), tag, Some(Ref(schema))).materializeRun)
            }
          }
        } ~
          (evalBool(withAttachment) & pathPrefix("attachments" / Segment) & pathEndOrSingleSlash) { filename =>
            // get a resource attachment
            (get & callerIdentity & hasPermission(resourceRead)) { implicit ident =>
              val result = (revOpt, tagOpt) match {
                case (None, None) =>
                  resources.fetchAttachment(Id(proj.ref, id), Some(Ref(schema)), filename).toEitherRun
                case (Some(_), Some(_)) => Future.successful(Left(simultaneousParamsRejection): RejectionOrAttachment)
                case (Some(rev), _) =>
                  resources.fetchAttachment(Id(proj.ref, id), rev, Some(Ref(schema)), filename).toEitherRun
                case (_, Some(tag)) =>
                  resources.fetchAttachment(Id(proj.ref, id), tag, Some(Ref(schema)), filename).toEitherRun
              }
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

  private val simultaneousParamsRejection: AkkaRejection =
    validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously.")

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun: Future[Option[ResourceV]] =
      resource.flatMap(resources.materializeWithMeta(_).toOption).value.runAsync
  }

  private def toQueryResults(resolvers: List[Resolver]): QueryResults[Resolver] =
    UnscoredQueryResults(resolvers.size.toLong, resolvers.map(UnscoredQueryResult(_)))

  private type RejectionOrAttachment = Either[AkkaRejection, Option[(Attachment.BinaryAttributes, AkkaOut)]]
  private implicit class OptionTaskAttachmentSyntax(resource: OptionT[Task, (Attachment.BinaryAttributes, AkkaOut)]) {
    def toEitherRun: Future[RejectionOrAttachment] =
      resource.value.map[RejectionOrAttachment](Right.apply).runAsync
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

  private implicit class JsonRoutesSyntax(json: Json) {
    def addContext(uriOpt: Option[AbsoluteIri]): Json = uriOpt.map(uri => json.addContext(uri)).getOrElse(json)
  }

  private implicit def resourceEncoder: Encoder[Resource] = Encoder.encodeJson.contramap { res =>
    val graph       = res.metadata ++ res.typeGraph
    val primaryNode = Some(IriNode(res.id.value))
    graph.asJson(resourceCtx, primaryNode).getOrElse(graph.asJson).removeKeys("@context").addContext(resourceCtxUri)
  }

  private implicit def resourceVEncoder: Encoder[ResourceV] = Encoder.encodeJson.contramap { res =>
    val graph       = res.value.graph
    val mergedCtx   = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
    val primaryNode = Some(IriNode(res.id.value))
    val jsonResult  = graph.asJson(mergedCtx, primaryNode).getOrElse(graph.asJson)
    jsonResult deepMerge Json.obj("@context" -> res.value.source.contextValue).addContext(resourceCtxUri)
  }

  private implicit def qrResolverGraphEncoder: GraphEncoder[QueryResult[Resolver]] =
    GraphEncoder(res => IriNode(res.source.id) -> res.source.asGraph)

  private implicit def qrResolverEncoder: Encoder[QueryResults[Resolver]] =
    qrsEncoder[Resolver](resolverCtx) mapJson (_ addContext resolverCtxUri)

  private def evalBool(value: Boolean): Directive0 =
    if (value) pass
    else reject

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
                                              config: AppConfig): ResourceRoutes =
    new ResourceRoutes(resources)

  private[routes] val resourceRead   = Permissions(Permission("resources/read"), Permission("resources/manage"))
  private[routes] val resourceWrite  = Permissions(Permission("resources/write"), Permission("resources/manage"))
  private[routes] val resourceCreate = Permissions(Permission("resources/create"), Permission("resources/manage"))
}
