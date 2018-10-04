package ch.epfl.bluebrain.nexus.kg.routes

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.data.EitherT
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.DeprecatedId._
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.marshallers.{ExceptionHandling, RejectionHandling}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, UnexpectedState}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.{marshallers, DeprecatedId}
import ch.epfl.bluebrain.nexus.rdf.Graph
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
                                                  aclsOps: AclsOps,
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
    def resolverRoute(acls: FullAccessControlList, caller: Caller)(implicit wrapped: LabeledProject): Route =
      new ResourceRoutes(resources, resolverSchemaUri, "resolvers", acls, caller) {

        override def transformGet(resource: ResourceV) =
          Resolver(resource, wrapped.accountRef) match {
            case Some(r) =>
              val metadata  = resource.metadata ++ resource.typeTriples
              val resValueF = r.labeled.getOrElse(r).map(_.resourceValue(resource.id, resource.value.ctx))
              resValueF.map(v => resource.map(_ => v.copy(graph = v.graph ++ Graph(metadata))))
            case _ => Task.pure(resource)
          }

        override def transformCreate(j: Json) =
          j.addContext(resolverCtxUri)

        override def transformUpdate(@silent id: AbsoluteIri, j: Json) =
          EitherT.rightT(transformCreate(j))

        override implicit def additional =
          AdditionalValidation.resolver(caller, wrapped.accountRef)

        override def list: Route =
          (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) {
            deprecated =>
              trace("listResolvers") {
                val qr = filterDeprecated(cache.resolvers(wrapped.ref), deprecated)
                  .flatMap(list => (list.flatTraverse(_.labeled.value.map(_.toList))))
                  .map(r => toQueryResults(r.sortBy(_.priority)))
                complete(qr.runAsync)
              }
          }
      }.routes

    (pathPrefix("resolvers") & project) { implicit wrapped =>
      (acls & caller)(resolverRoute)
    } ~ (pathPrefix("resources") & project) { implicit wrapped =>
      (isIdSegment(resolverSchemaUri) & acls & caller)(resolverRoute)
    }
  }

  private def view(implicit token: Option[AuthToken]): Route = {
    val resourceRead = Permissions(Permission("views/read"), Permission("views/manage"))

    implicit val um = marshallers.sparqlQueryUnmarshaller

    def search(implicit acls: FullAccessControlList, caller: Caller, wrapped: LabeledProject) =
      (pathPrefix(IdSegment / "sparql") & post & entity(as[String]) & pathEndOrSingleSlash & hasPermission(
        resourceRead)) { (id, query) =>
        val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
          views.find(_.id == id) match {
            case Some(v: SparqlView) => sparql.copy(namespace = v.name).queryRaw(query).map(Right.apply)
            case _                   => Task.pure(Left(NotFound(Ref(id))))
          }
        }
        trace("searchSparql")(complete(result.runAsync))
      } ~
        (pathPrefix(IdSegment / "_search") & post & entity(as[Json]) & extract(_.request.uri.query()) & pathEndOrSingleSlash & hasPermission(
          resourceRead)) { (id, query, params) =>
          val result: Task[Either[Rejection, Json]] = cache.views(wrapped.ref).flatMap { views =>
            views.find(_.id == id) match {
              case Some(v: ElasticView) => es.searchRaw(query, Set(v.index), params).map(Right(_))
              case _                    => Task.pure(Left(NotFound(Ref(id))))
            }
          }
          trace("searchElastic")(complete(result.runAsync))
        }

    def viewRoutes(acls: FullAccessControlList, caller: Caller)(implicit wrapped: LabeledProject): Route =
      new ResourceRoutes(resources, viewSchemaUri, "views", acls, caller) {

        override implicit def additional = AdditionalValidation.view

        private def transformView(source: Json, uuid: String): Json = {
          val transformed = source deepMerge Json.obj(nxv.uuid.prefix -> Json.fromString(uuid)).addContext(viewCtxUri)
          transformed.hcursor.get[Json]("mapping") match {
            case Right(m) if m.isObject => transformed deepMerge Json.obj("mapping" -> Json.fromString(m.noSpaces))
            case _                      => transformed
          }
        }

        override def list: Route =
          (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) {
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
                r.value.hcursor.get[String](nxv.uuid.prefix).left.map(_ => UnexpectedState(resId.ref): Rejection)))
            .map(uuid => transformView(j, uuid))
        }
      }.routes

    (pathPrefix("views") & project) { implicit wrapped =>
      (acls & caller) { (fullAcls, c) =>
        viewRoutes(fullAcls, c) ~ search(fullAcls, c, wrapped)
      }
    } ~ (pathPrefix("resources") & project) { implicit wrapped =>
      (isIdSegment(viewSchemaUri) & acls & caller) { (fullAcls, c) =>
        viewRoutes(fullAcls, c) ~ search(fullAcls, c, wrapped)
      }
    }
  }

  private def schema(implicit token: Option[AuthToken]): Route = {
    def schemaRoutes(acls: FullAccessControlList, caller: Caller)(implicit wrapped: LabeledProject): Route =
      new ResourceRoutes(resources, shaclSchemaUri, "schemas", acls, caller) {

        override def transformCreate(j: Json): Json =
          j.addContext(shaclCtxUri)

        override def transformUpdate(id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] =
          EitherT.rightT(transformCreate(j))
      }.routes

    (pathPrefix("schemas") & project) { implicit wrapped =>
      (acls & caller)(schemaRoutes)
    } ~ (pathPrefix("resources") & project) { implicit wrapped =>
      (isIdSegment(shaclSchemaUri) & acls & caller)(schemaRoutes)
    }
  }

  private def resource(implicit token: Option[AuthToken]): Route = {
    val resourceRead = Permissions(Permission("resources/read"), Permission("resources/manage"))
    (pathPrefix("resources") & project) { implicit wrapped =>
      acls.apply { implicit acl =>
        caller.apply { implicit c =>
          (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
            (deprecated, pagination) =>
              trace("listResources") {
                complete(cache.views(wrapped.ref).flatMap(v => resources.list(v, deprecated, pagination)).runAsync)
              }
          } ~
            pathPrefix(IdSegment) { schema =>
              new ResourceRoutes(resources, schema, "resources", acl, c).routes
            }
        }
      }
    }
  }

  private def filterDeprecated[A: DeprecatedId](set: Task[Set[A]], deprecated: Option[Boolean]): Task[List[A]] =
    set.map(s => deprecated.map(d => s.filter(_.deprecated == d)).getOrElse(s).toList)

  private def toQueryResults[A](resolvers: List[A]): QueryResults[A] =
    UnscoredQueryResults(resolvers.size.toLong, resolvers.map(UnscoredQueryResult(_)))
}
