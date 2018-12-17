package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.javadsl.server.Rejections.validationRejection
import akka.http.scaladsl.model.StatusCodes.Created
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, Rejection => AkkaRejection}
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
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
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import com.github.ghik.silencer.silent
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

private sealed abstract class ResourceRoutes(resources: Resources[Task],
                                             schemaRef: Option[Ref],
                                             prefix: String,
                                             acls: FullAccessControlList,
                                             caller: Caller)(implicit wrapped: LabeledProject, config: AppConfig) {

  implicit val acl = acls
  implicit val c   = caller

  private[routes] val suffixTracing = prefix.capitalize

  def transformCreate(j: Json): Json = j

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def transformUpdate(@silent id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] =
    EitherT.rightT(j)

  def transformGet(resource: ResourceV): Task[ResourceV] =
    Task.pure(resource)

  implicit def additional: AdditionalValidation[Task] =
    AdditionalValidation.pass

  def update(id: AbsoluteIri): Route =
    (put & entity(as[Json]) & projectNotDeprecated & hasPermission(resourceWrite) & parameter('rev.as[Long]) & pathEndOrSingleSlash) {
      (json, rev) =>
        identity.apply { implicit ident =>
          trace(s"update$suffixTracing") {
            complete(
              transformUpdate(id, json)
                .flatMap(transformed => resources.update(Id(wrapped.ref, id), rev, schemaRef, transformed))
                .value
                .runToFuture)
          }
        }
    }

  def tag(id: AbsoluteIri): Route =
    (pathPrefix("tags") & projectNotDeprecated & put & entity(as[Json]) & hasPermission(resourceWrite) & parameter(
      'rev.as[Long]) & pathEndOrSingleSlash) { (json, rev) =>
      identity.apply { implicit ident =>
        trace(s"addTag$suffixTracing") {
          val tagged = resources.tag(Id(wrapped.ref, id), rev, schemaRef, json.addContext(tagCtxUri))
          complete(Created -> tagged.value.runToFuture)
        }
      }
    }

  def deprecate(id: AbsoluteIri): Route =
    (delete & projectNotDeprecated & hasPermission(resourceWrite) & parameter('rev.as[Long]) & pathEndOrSingleSlash) {
      rev =>
        identity.apply { implicit ident =>
          trace(s"deprecate$suffixTracing") {
            complete(resources.deprecate(Id(wrapped.ref, id), rev, schemaRef).value.runToFuture)
          }
        }
    }

  def getResource(id: AbsoluteIri): Route =
    (get & parameter('rev.as[Long].?) & parameter('tag.?) & pathEndOrSingleSlash & hasPermission(resourceRead)) {
      (revOpt, tagOpt) =>
        trace(s"get$suffixTracing") {
          (revOpt, tagOpt) match {
            case (None, None) =>
              complete(resources.fetch(Id(wrapped.ref, id), schemaRef).materializeRun(Ref(id)))
            case (Some(_), Some(_)) =>
              reject(simultaneousParamsRejection)
            case (Some(rev), _) =>
              complete(resources.fetch(Id(wrapped.ref, id), rev, schemaRef).materializeRun(Ref(id)))
            case (_, Some(tag)) =>
              complete(resources.fetch(Id(wrapped.ref, id), tag, schemaRef).materializeRun(Ref(id)))
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
      } yield transformed).value.runToFuture
  }

  private[routes] val resourceRead =
    Permissions(Permission(s"$prefix/read"), Permission(s"$prefix/manage"))
  private[routes] val resourceWrite =
    Permissions(Permission(s"$prefix/write"), Permission(s"$prefix/manage"))
  private[routes] val resourceCreate =
    Permissions(Permission(s"$prefix/create"), Permission(s"$prefix/manage"))

}

object ResourceRoutes {

  private[routes] class Schemed(resources: Resources[Task],
                                schema: AbsoluteIri,
                                prefix: String,
                                acls: FullAccessControlList,
                                caller: Caller)(implicit wrapped: LabeledProject,
                                                cache: DistributedCache[Task],
                                                indexers: Clients[Task],
                                                config: AppConfig)
      extends ResourceRoutes(resources, Some(Ref(schema)), prefix, acls, caller) {

    import indexers._

    def routes: Route =
      create ~ list ~ pathPrefix(IdSegment) { id =>
        update(id) ~ createWithId(id) ~ tag(id) ~ deprecate(id) ~ getResource(id)
      }

    def list: Route =
      (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
        (deprecated, pagination) =>
          trace(s"list$suffixTracing") {
            complete(
              cache.views(wrapped.ref).flatMap(v => resources.list(v, deprecated, schema, pagination)).runToFuture)
          }
      }

    def create: Route =
      (projectNotDeprecated & post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
        (identity & hasPermission(resourceCreate)) { implicit ident =>
          trace(s"create$suffixTracing") {
            val created = resources.create(wrapped.ref, wrapped.base, Ref(schema), transformCreate(source))
            complete(Created -> created.value.runToFuture)
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
                .runToFuture)
          }
        }
      }
  }

  private[routes] class Unschemed(resources: Resources[Task],
                                  prefix: String,
                                  acls: FullAccessControlList,
                                  caller: Caller)(implicit wrapped: LabeledProject, config: AppConfig)
      extends ResourceRoutes(resources, None, prefix, acls, caller) {

    def routes: Route =
      pathPrefix(IdSegment) { id =>
        update(id) ~ tag(id) ~ deprecate(id) ~ getResource(id)
      }
  }

}
