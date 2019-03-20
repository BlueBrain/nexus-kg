package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCode}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{MalformedQueryParamRejection, Route, Rejection => AkkaRejection}
import cats.data.EitherT
import cats.instances.future._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.search.Pagination
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.KgError.UnacceptedResponseContentType
import ch.epfl.bluebrain.nexus.kg.cache.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.storage.{AkkaSource, Storage}
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Dot, NTriples}
import io.circe.syntax._
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

private[routes] abstract class CommonRoutes(
    resources: Resources[Task],
    prefix: String,
    acls: AccessControlLists,
    caller: Caller,
)(implicit project: Project, indexers: Clients[Task], config: AppConfig, viewCache: ViewCache[Task]) {

  import indexers._

  protected implicit val acl: AccessControlLists                = acls
  protected implicit val c: Caller                              = caller
  protected implicit val subject: Identity.Subject              = caller.subject
  protected implicit def additional: AdditionalValidation[Task] = AdditionalValidation.pass

  private[routes] val resourceName = {
    val c = prefix.capitalize
    if (c.endsWith("s")) c.dropRight(1) else c
  }

  private val wrongSchema: AkkaRejection =
    MalformedQueryParamRejection("schema", "The provided schema does not match the schema on the Uri")

  protected val read: Permission = Permission.unsafe("resources/read")

  protected val write: Permission = Permission.unsafe(s"$prefix/write")

  /**
    * Performs transformations on the retrieved resource from the primary store
    * in order to present it back to the client
    *
    * @param r the resource
    * @return the transformed resource wrapped on the effect type ''F''
    */
  def transform(r: ResourceV): Task[ResourceV] = Task.pure(r)

  /**
    * Transforms the incoming client payload
    *
    * @param payload the client payload
    * @return a new Json with the transformed payload
    */
  def transform(payload: Json): Json = payload

  def routes: Route

  def create(schema: Ref): Route =
    (post & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
      entity(as[Json]) { source =>
        trace(s"create$resourceName") {
          complete(resources.create(project.base, schema, transform(source)).value.runWithStatus(Created))
        }
      }
    }

  def create(id: AbsoluteIri, schema: Ref): Route =
    (put & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) {
      entity(as[Json]) { source =>
        trace(s"create$resourceName") {
          complete(resources.create(Id(project.ref, id), schema, transform(source)).value.runWithStatus(Created))
        }
      }
    }

  def update(id: AbsoluteIri, schema: Ref): Route =
    (put & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
      entity(as[Json]) { source =>
        trace(s"update$resourceName") {
          complete(resources.update(Id(project.ref, id), rev, schema, transform(source)).value.runWithStatus(OK))
        }
      }
    }

  def tag(id: AbsoluteIri, schema: Ref): Route =
    pathPrefix("tags") {
      (post & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
        entity(as[Json]) { source =>
          trace(s"addTag$resourceName") {
            val tagged = resources.tag(Id(project.ref, id), rev, schema, source.addContext(tagCtxUri))
            complete(tagged.value.runWithStatus(Created))
          }
        }
      }
    }

  def tags(id: AbsoluteIri, schema: Ref): Route =
    pathPrefix("tags") {
      (get & parameter('rev.as[Long].?) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(read)) {
        case Some(rev) => complete(resources.fetchTags(Id(project.ref, id), rev, schema).value.runWithStatus(OK))
        case _         => complete(resources.fetchTags(Id(project.ref, id), schema).value.runWithStatus(OK))
      }
    }

  def deprecate(id: AbsoluteIri, schema: Ref): Route =
    (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermission(write)) { rev =>
      trace(s"deprecate$resourceName") {
        complete(resources.deprecate(Id(project.ref, id), rev, schema).value.runWithStatus(OK))
      }
    }

  def fetch(id: AbsoluteIri, schema: Ref): Route = {
    val defaultOutput: OutputFormat = if (schema == fileRef) Binary else Compacted
    (get & outputFormat(defaultOutput == Binary, defaultOutput) & pathEndOrSingleSlash) {
      case Binary                        => getFile(id, schema)
      case format: NonBinaryOutputFormat => getResource(id, schema)(format)
    }
  }

  private def getResource(id: AbsoluteIri, schema: Ref)(implicit format: NonBinaryOutputFormat): Route =
    hasPermission(read).apply {
      trace(s"get$resourceName") {
        val idRes = Id(project.ref, id)
        concat(
          (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
            completeWithFormat(resources.fetch(idRes, rev, schema).materializeRun)
          },
          (parameter('tag) & noParameter('rev)) { tag =>
            completeWithFormat(resources.fetch(idRes, tag, schema).materializeRun)
          },
          (noParameter('tag) & noParameter('rev)) {
            completeWithFormat(resources.fetch(idRes, schema).materializeRun)
          }
        )
      }
    }

  private def completeWithFormat(fetched: EitherT[Future, Rejection, (StatusCode, ResourceV)])(
      implicit format: NonBinaryOutputFormat): Route =
    format match {
      case f: JsonLDOutputFormat =>
        implicit val format = f
        complete(fetched.value)
      case Triples =>
        implicit val format = Triples
        complete(fetched.map { case (status, resource) => status -> resource.value.graph.as[NTriples]().value }.value)
      case DOT =>
        implicit val format = DOT
        complete(fetched.map { case (status, resource) => status -> resource.value.graph.as[Dot]().value }.value)
    }

  private def getFile(id: AbsoluteIri, schema: Ref): Route =
    trace("getFile") {
      extractActorSystem { implicit as =>
        concat(
          (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
            completeFile(resources.fetchFile(Id(project.ref, id), rev, schema).value.runToFuture)
          },
          (parameter('tag) & noParameter('rev)) { tag =>
            completeFile(resources.fetchFile(Id(project.ref, id), tag, schema).value.runToFuture)
          },
          (noParameter('tag) & noParameter('rev)) {
            completeFile(resources.fetchFile(Id(project.ref, id), schema).value.runToFuture)
          }
        )
      }
    }

  private def completeFile(f: Future[Either[Rejection, (Storage, FileAttributes, AkkaSource)]]): Route =
    onSuccess(f) {
      case Right((storage, info, source)) =>
        hasPermission(storage.readPermission).apply {
          val filename = urlEncodeOrElse(info.filename)("file")
          (respondWithHeaders(RawHeader("Content-Disposition", s"attachment; filename*=UTF-8''$filename")) & encodeResponse) {
            headerValueByType[Accept](()) { accept =>
              val contentType = ContentType.parse(info.mediaType).getOrElse(Binary.contentType)
              if (accept.mediaRanges.exists(_.matches(contentType.mediaType)))
                complete(HttpEntity(contentType, info.bytes, source))
              else
                failWith(
                  UnacceptedResponseContentType(
                    s"File Media Type '$contentType' does not match the Accept header value '${accept.mediaRanges
                      .mkString(", ")}'"))
            }
          }
        }
      case Left(err) => complete(err)
    }

  def list: Route =
    (get & paginated & searchParams & pathEndOrSingleSlash & hasPermission(read)) { (pagination, params) =>
      trace(s"list$resourceName") {
        complete(list(pagination, params))
      }
    }

  def list(schema: Ref): Route =
    (get & paginated & searchParams & pathEndOrSingleSlash & hasPermission(read)) { (pagination, params) =>
      if (params.schema.getOrElse(schema.iri) != schema.iri) reject(wrongSchema)
      else {
        trace(s"list$resourceName") {
          complete(list(pagination, params.copy(schema = Some(schema.iri))))
        }
      }
    }

  private def list(pagination: Pagination, params: SearchParams): Future[(StatusCode, JsonResults)] =
    viewCache
      .getBy[ElasticSearchView](project.ref, nxv.defaultElasticSearchIndex.value)
      .flatMap(resources.list(_, params, pagination))
      .runWithStatus(OK)

  private implicit class OptionTaskSyntax(rejOrResource: EitherT[Task, Rejection, Resource]) {
    def materializeRun: EitherT[Future, Rejection, (StatusCode, ResourceV)] =
      EitherT((for {
        res          <- rejOrResource
        materialized <- resources.materializeWithMeta(res)
        transformed  <- EitherT.right[Rejection](transform(materialized))
      } yield transformed).value.runWithStatus(OK))
  }

  private implicit def tagsEncoder: Encoder[Tags] =
    Encoder.instance(tags => Json.obj(nxv.tags.prefix -> Json.arr(tags.map(_.asJson).toSeq: _*)).addContext(tagCtxUri))
}
