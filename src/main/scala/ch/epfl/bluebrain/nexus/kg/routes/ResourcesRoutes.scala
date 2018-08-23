package ch.epfl.bluebrain.nexus.kg.routes

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.DeprecatedId._
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
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
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import com.github.ghik.silencer.silent
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

/**
  * Routes for resources operations
  *
  * @param resources the resources operations
  */
class ResourcesRoutes(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                                  indexers: Clients[Task],
                                                  store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                                  config: AppConfig) {

  private val (es, sparql) = (indexers.elastic, indexers.sparql)
  import indexers._

  def routes: Route =
    handleRejections(RejectionHandling()) {
      handleExceptions(ExceptionHandling()) {
        token { implicit optToken =>
          pathPrefix(config.http.prefix) {
            resolver ~ view ~ schema ~ resource
          }
        }
      }
    }

  private def resolver(implicit token: Option[AuthToken]): Route = {
    def resolverRoute(implicit wrapped: LabeledProject, acls: Option[FullAccessControlList]): Route =
      new ResourceRoutes(resources, resolverSchemaUri, "resolvers") {
        override def transformCreate(j: Json) =
          j.addContext(resolverCtxUri)

        override def transformUpdate(@silent id: AbsoluteIri, j: Json) =
          EitherT.rightT(transformCreate(j))

        override implicit def additional =
          AdditionalValidation.resolver(acls, wrapped.accountRef, cache.projectRef)

        override def list: Route =
          (get & parameter('deprecated.as[Boolean].?) & hasPermissionInAcl(resourceRead) & pathEndOrSingleSlash) {
            deprecated =>
              trace("listResolvers") {
                val qr = filterDeprecated(cache.resolvers(wrapped.ref), deprecated).map { r =>
                  toQueryResults(r.sortBy(_.priority))
                }
                complete(qr.runAsync)
              }
          }
      }.routes

    (pathPrefix("resolvers") & project) { implicit wrapped =>
      acls.apply(implicit acls => resolverRoute)
    } ~ (pathPrefix("resources") & project) { implicit wrapped =>
      pathPrefix(isIdSegment(resolverSchemaUri)) {
        acls.apply(implicit acls => resolverRoute)
      }
    }
  }

  private def view(implicit token: Option[AuthToken]): Route = {
    val resourceRead = Permissions(Permission("views/read"), Permission("views/manage"))

    def search(implicit wrapped: LabeledProject, acls: Option[FullAccessControlList]) =
      (pathPrefix(IdSegment / "sparql") & post & entity(as[String]) & pathEndOrSingleSlash) { (id, query) =>
        (callerIdentity & hasPermissionInAcl(resourceRead)) { implicit ident =>
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
            (callerIdentity & hasPermissionInAcl(resourceRead)) { implicit ident =>
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

    def viewRoutes(implicit wrapped: LabeledProject, acls: Option[FullAccessControlList]): Route =
      new ResourceRoutes(resources, viewSchemaUri, "views") {

        override implicit def additional = AdditionalValidation.view

        private def transformView(source: Json, uuid: String): Json = {
          val transformed = source deepMerge Json.obj("uuid" -> Json.fromString(uuid)).addContext(viewCtxUri)
          transformed.hcursor.get[Json]("mapping") match {
            case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
            case _                      => transformed
          }
        }

        override def list: Route =
          (get & parameter('deprecated.as[Boolean].?) & hasPermissionInAcl(resourceRead) & pathEndOrSingleSlash) {
            deprecated =>
              trace("listViews") {
                complete(filterDeprecated(cache.views(wrapped.ref), deprecated).map(toQueryResults).runAsync)
              }
          }

        override def transformCreate(j: Json): Json =
          transformView(j, UUID.randomUUID().toString.toLowerCase)

        override def transformUpdate(id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] = {
          val resId = Id(wrapped.ref, id)
          resources
            .fetch(resId, Some(Latest(viewSchemaUri)))
            .toRight(NotFound(resId.ref): Rejection)
            .flatMap(r =>
              EitherT.fromEither(
                r.value.hcursor.get[String]("uuid").left.map(_ => UnexpectedState(resId.ref): Rejection)))
            .map(uuid => transformView(j, uuid))
        }
      }.routes

    (pathPrefix("views") & project) { implicit wrapped =>
      acls.apply(implicit acls => viewRoutes ~ search)
    } ~ (pathPrefix("resources") & project) { implicit wrapped =>
      pathPrefix(isIdSegment(viewSchemaUri)) {
        acls.apply(implicit acls => viewRoutes ~ search)
      }
    }
  }

  private def schema(implicit token: Option[AuthToken]): Route =
    ResourceRoutes(resources, shaclSchemaUri, "schemas")

  private def resource(implicit token: Option[AuthToken]): Route =
    (pathPrefix("resources") & project) { implicit wrapped =>
      acls.apply { implicit acls =>
        pathPrefix(IdSegment) { schema =>
          new ResourceRoutes(resources, schema, "resources").routes
        }
      }
    }

  private def filterDeprecated[A: DeprecatedId](set: Task[Set[A]], deprecated: Option[Boolean]): Task[List[A]] =
    set.map(s => deprecated.map(d => s.filter(_.deprecated == d)).getOrElse(s).toList)

  private def toQueryResults[A](resolvers: List[A]): QueryResults[A] =
    UnscoredQueryResults(resolvers.size.toLong, resolvers.map(UnscoredQueryResult(_)))
}
