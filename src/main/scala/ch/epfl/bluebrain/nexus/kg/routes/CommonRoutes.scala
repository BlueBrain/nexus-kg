package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
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
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

private[routes] abstract class CommonRoutes(
    resources: Resources[Task],
    prefix: String,
    acls: AccessControlLists,
    caller: Caller,
    viewCache: ViewCache[Task])(implicit project: Project, indexers: Clients[Task], config: AppConfig) {

  import indexers._

  protected implicit val acl: AccessControlLists                = acls
  protected implicit val c: Caller                              = caller
  protected implicit val subject: Identity.Subject              = caller.subject
  protected implicit def additional: AdditionalValidation[Task] = AdditionalValidation.pass

  private[routes] val resourceName = prefix.capitalize

  private[routes] val simultaneousParamsRejection: AkkaRejection =
    validationRejection("'rev' and 'tag' query parameters cannot be present simultaneously.")

  protected val readPermission: Set[Permission] = Set(Permission.unsafe("resources/read"))

  protected val writePermission: Set[Permission] = Set(Permission.unsafe(s"$prefix/write"))

  def transform(r: ResourceV): Task[ResourceV] = Task.pure(r)

  def routes: Route

  def create(schema: Ref): Route =
    (post & entity(as[Json]) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) { source =>
      trace(s"create$resourceName") {
        complete(Created -> resources.create(project.ref, project.base, schema, source).value.runToFuture)
      }
    }

  def create(id: AbsoluteIri, schema: Ref): Route =
    (put & entity(as[Json]) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) { source =>
      trace(s"create$resourceName") {
        complete(Created -> resources.create(Id(project.ref, id), schema, source).value.runToFuture)
      }
    }

  def update(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (put & entity(as[Json]) & parameter('rev.as[Long].?) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
      case (source, Some(rev)) =>
        trace(s"update$resourceName") {
          complete(resources.update(Id(project.ref, id), rev, schemaOpt, source).value.runToFuture)
        }
      case (_, None) => reject()
    }

  def tag(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    pathPrefix("tags") {
      (put & entity(as[Json]) & parameter('rev.as[Long]) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
        (json, rev) =>
          trace(s"addTag$resourceName") {
            val tagged = resources.tag(Id(project.ref, id), rev, schemaOpt, json.addContext(tagCtxUri))
            complete(Created -> tagged.value.runToFuture)
          }
      }
    }

  def tags(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    pathPrefix("tags") {
      (get & parameter('rev.as[Long].?) & projectNotDeprecated & hasPermission(readPermission) & pathEndOrSingleSlash) {
        revOpt =>
          val tags = revOpt
            .map(rev => resources.fetchTags(Id(project.ref, id), rev, schemaOpt))
            .getOrElse(resources.fetchTags(Id(project.ref, id), schemaOpt))
          complete(tags.value.runToFuture)
      }
    }

  def deprecate(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (delete & parameter('rev.as[Long]) & projectNotDeprecated & hasPermission(writePermission) & pathEndOrSingleSlash) {
      rev =>
        trace(s"deprecate$resourceName") {
          complete(resources.deprecate(Id(project.ref, id), rev, schemaOpt).value.runToFuture)
        }
    }

  def fetch(id: AbsoluteIri, schemaOpt: Option[Ref]): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & hasPermission(readPermission) & pathEndOrSingleSlash) {
      (revOpt, tagOpt) =>
        val idRes = Id(project.ref, id)
        trace(s"get$resourceName") {
          (revOpt, tagOpt) match {
            case (Some(_), Some(_)) => reject(simultaneousParamsRejection)
            case (Some(rev), _)     => complete(resources.fetch(idRes, rev, schemaOpt).materializeRun(id.ref))
            case (_, Some(tag))     => complete(resources.fetch(idRes, tag, schemaOpt).materializeRun(id.ref))
            case _                  => complete(resources.fetch(idRes, schemaOpt).materializeRun(id.ref))
          }
        }
    }

  def list(schemaOpt: Option[Ref]): Route =
    (get & paginated & searchParams & hasPermission(readPermission) & pathEndOrSingleSlash) { (pagination, params) =>
      val schema = schemaOpt.map(_.iri).orElse(params.schema)
      trace(s"list$resourceName") {
        complete(
          viewCache
            .getBy[ElasticView](project.ref, nxv.defaultElasticIndex.value)
            .flatMap(v => resources.list(v, params.copy(schema = schema), pagination))
            .runToFuture)
      }
    }

  private implicit class OptionTaskSyntax(resource: OptionT[Task, Resource]) {
    def materializeRun(ref: => Ref): Future[Either[Rejection, ResourceV]] =
      (for {
        res          <- resource.toRight(NotFound(ref): Rejection)
        materialized <- resources.materializeWithMeta(res)
        transformed  <- EitherT.right[Rejection](transform(materialized))
      } yield transformed).value.runToFuture
  }

  private implicit val tagsEncoder: Encoder[Tags] = Encoder.instance { tags =>
    val arr = tags.foldLeft(List.empty[Json]) {
      case (acc, (tag, rev)) =>
        Json.obj(nxv.tag.prefix -> Json.fromString(tag), "rev" -> Json.fromLong(rev)) :: acc
    }
    Json.obj(nxv.tags.prefix -> Json.arr(arr: _*)).addContext(tagCtxUri)
  }

}
