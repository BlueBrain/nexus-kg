package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.{ContentType, HttpEntity, StatusCode}
import akka.http.scaladsl.model.StatusCodes.{Created, OK}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import cats.syntax.show._
import akka.http.scaladsl.server.{MalformedQueryParamRejection, Route, Rejection => AkkaRejection}
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.dot._
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat._
import ch.epfl.bluebrain.nexus.kg.urlEncodeOrElse
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}

import scala.concurrent.Future

private[routes] abstract class CommonRoutes(resources: Resources[Task],
                                            prefix: String,
                                            acls: AccessControlLists,
                                            caller: Caller,
                                            viewCache: ViewCache[Task])(implicit project: Project,
                                                                        indexers: Clients[Task],
                                                                        config: AppConfig,
                                                                        store: FileStore[Task, AkkaIn, AkkaOut]) {

  import indexers._

  protected implicit val acl: AccessControlLists                = acls
  protected implicit val c: Caller                              = caller
  protected implicit val subject: Identity.Subject              = caller.subject
  protected implicit def additional: AdditionalValidation[Task] = AdditionalValidation.pass

  private[routes] val resourceName = {
    val c = prefix.capitalize
    if (c.endsWith("s")) c.dropRight(1) else c
  }

  private[routes] val simultaneousParamsRejection: AkkaRejection =
    MalformedQueryParamRejection("rev", "'rev' and 'tag' query parameters cannot be present simultaneously")

  protected val read: Set[Permission] = Set(Permission.unsafe("resources/read"))

  protected val write: Set[Permission] = Set(Permission.unsafe(s"$prefix/write"))

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
    (post & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) {
      entity(as[Json]) { source =>
        trace(s"create$resourceName") {
          complete(resources.create(project.ref, project.base, schema, transform(source)).value.runWithStatus(Created))
        }
      }
    }

  def create(id: AbsoluteIri, schema: Ref): Route =
    (put & noParameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) {
      entity(as[Json]) { source =>
        trace(s"create$resourceName") {
          complete(resources.create(Id(project.ref, id), schema, transform(source)).value.runWithStatus(Created))
        }
      }
    }

  def update(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (put & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) { rev =>
      entity(as[Json]) { source =>
        trace(s"update$resourceName") {
          complete(resources.update(Id(project.ref, id), rev, schemaOpt, transform(source)).value.runWithStatus(OK))
        }
      }
    }

  def tag(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    pathPrefix("tags") {
      (post & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) { rev =>
        entity(as[Json]) { source =>
          trace(s"addTag$resourceName") {
            val tagged = resources.tag(Id(project.ref, id), rev, schemaOpt, source.addContext(tagCtxUri))
            complete(tagged.value.runWithStatus(Created))
          }
        }
      }
    }

  def tags(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    pathPrefix("tags") {
      (get & parameter('rev.as[Long].?) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(read)) {
        revOpt =>
          val tags = revOpt
            .map(rev => resources.fetchTags(Id(project.ref, id), rev, schemaOpt))
            .getOrElse(resources.fetchTags(Id(project.ref, id), schemaOpt))
          complete(tags.value.runNotFound(id.ref))
      }
    }

  def deprecate(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (delete & parameter('rev.as[Long]) & projectNotDeprecated & pathEndOrSingleSlash & hasPermissions(write)) { rev =>
      trace(s"deprecate$resourceName") {
        complete(resources.deprecate(Id(project.ref, id), rev, schemaOpt).value.runWithStatus(OK))
      }
    }

  def fetch(id: AbsoluteIri, schemaOpt: Option[Ref]): Route = {
    val defaultOutput: OutputFormat = schemaOpt.collect { case `fileRef` => Binary }.getOrElse(Compacted)
    (get & outputFormat(defaultOutput == Binary, defaultOutput) & pathEndOrSingleSlash & hasPermissions(read)) {
      case Binary                        => getFile(id)
      case output: NonBinaryOutputFormat => getResource(id, schemaOpt)(output)
    }
  }

  private def getResource(id: AbsoluteIri, schemaOpt: Option[Ref])(implicit output: NonBinaryOutputFormat): Route =
    trace(s"get$resourceName") {
      val idRes = Id(project.ref, id)
      concat(
        (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
          completeWithFormat(resources.fetch(idRes, rev, schemaOpt).materializeRun(id.ref, Some(rev), None))
        },
        (parameter('tag) & noParameter('rev)) { tag =>
          completeWithFormat(resources.fetch(idRes, tag, schemaOpt).materializeRun(id.ref, None, Some(tag)))
        },
        (noParameter('tag) & noParameter('rev)) {
          completeWithFormat(resources.fetch(idRes, schemaOpt).materializeRun(id.ref, None, None))
        }
      )
    }

  private def completeWithFormat(fetched: Future[Either[Rejection, (StatusCode, ResourceV)]])(
      implicit output: NonBinaryOutputFormat): Route =
    output match {
      case f: JsonLDOutputFormat =>
        implicit val format = f
        complete(fetched)
      case Triples =>
        implicit val marshaller = stringMarshaller(Triples)
        complete(fetched.map(_.map {
          case (status, resource) =>
            status -> resource.value.graph.triples
              .map { case (s, p, o) => s"${s.show} ${p.show} ${o.show} ." }
              .mkString("\n")
        }))
      case DOT =>
        implicit val marshaller = stringMarshaller(DOT)
        complete(fetched.map(_.map {
          case (status, resource) =>
            status -> resource.value.graph.asDot
        }))
    }

  private def getFile(id: AbsoluteIri): Route =
    trace("getFile") {
      concat(
        (parameter('rev.as[Long]) & noParameter('tag)) { rev =>
          completeFile(resources.fetchFile(Id(project.ref, id), rev).value.runNotFound(id.ref))
        },
        (parameter('tag) & noParameter('rev)) { tag =>
          completeFile(resources.fetchFile(Id(project.ref, id), tag).value.runNotFound(id.ref))
        },
        (noParameter('tag) & noParameter('rev)) {
          completeFile(resources.fetchFile(Id(project.ref, id)).value.runNotFound(id.ref))
        }
      )
    }

  private def completeFile(f: Future[(FileAttributes, FileStore.AkkaOut)]): Route =
    onSuccess(f) {
      case (info, source) =>
        val filename = urlEncodeOrElse(info.filename)("file")
        (respondWithHeaders(RawHeader("Content-Disposition", s"attachment; filename*=UTF-8''$filename")) & encodeResponse) {
          val contentType = ContentType.parse(info.mediaType).getOrElse(Binary.contentType)
          complete(HttpEntity(contentType, info.bytes, source))
        }
    }

  def list(schemaOpt: Option[Ref]): Route =
    (get & paginated & searchParams & pathEndOrSingleSlash & hasPermissions(read)) { (pagination, params) =>
      val schema = schemaOpt.map(_.iri).orElse(params.schema)
      trace(s"list$resourceName") {
        complete(
          viewCache
            .getBy[ElasticSearchView](project.ref, nxv.defaultElasticSearchIndex.value)
            .flatMap(v => resources.list(v, params.copy(schema = schema), pagination))
            .runToFuture)
      }
    }

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun(ref: => Ref,
                       rev: Option[Long],
                       tag: Option[String]): Future[Either[Rejection, (StatusCode, ResourceV)]] =
      (for {
        res          <- resource.toRight(NotFound(ref, rev, tag): Rejection)
        materialized <- resources.materializeWithMeta(res)
        transformed  <- EitherT.right[Rejection](transform(materialized))
      } yield transformed).value.runWithStatus(OK)
  }

  private implicit val tagsEncoder: Encoder[Tags] = Encoder.instance { tags =>
    val arr = tags.foldLeft(List.empty[Json]) {
      case (acc, (tag, rev)) =>
        Json.obj(nxv.tag.prefix -> Json.fromString(tag), "rev" -> Json.fromLong(rev)) :: acc
    }
    Json.obj(nxv.tags.prefix -> Json.arr(arr: _*)).addContext(tagCtxUri)
  }

}
