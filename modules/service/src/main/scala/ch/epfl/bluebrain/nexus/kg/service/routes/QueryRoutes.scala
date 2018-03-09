package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{Uri, _}
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1, ExceptionHandler, Route}
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.kamon.directives.TracingDirectives
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextName, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organization}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.core.queries.{Queries, Query, QueryId, QueryResource}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.{FilterQueries, ResourceRestrictionExpr, SchemaNameFilterExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.{authenticateCaller, authorizeResource, getAcls}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.kg.service.routes.QueryRoutes._
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders._
import io.circe.Encoder

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for query specific functionality.
  *
  * @param queries       the bundle operations for queries
  * @param idsToEntities the bundle of mappings between ids and entities
  * @param base          the service public uri + prefix
  */
class QueryRoutes(queries: Queries[Future], idsToEntities: GroupedIdsToEntityRetrieval, base: Uri)(
    implicit
    contexts: Contexts[Future],
    client: SparqlQuery[Future],
    querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext,
    orderedKeys: OrderedKeys,
    prefixes: PrefixUris,
    tracing: TracingDirectives) {

  private val basePath: String                   = "queries"
  private val exceptionHandler: ExceptionHandler = ExceptionHandling.exceptionHandler(prefixes.ErrorContext)

  def routes: Route = handleExceptions(exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler(prefixes.ErrorContext)) {
      pathPrefix(basePath) {
        extractCredentials {
          case Some(c: OAuth2BearerToken) => searchRoutes(Some(c))
          case Some(_)                    => reject(AuthorizationFailedRejection)
          case _                          => searchRoutes(None)
        }
      }
    }
  }

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (post & extractResourcePath & paginated & queryEntity) { (path, pagination, query) =>
      (authenticateCaller & authorizeResource(path, Read)) { implicit caller =>
        onSuccess(queries.create(path, query)) { queryId =>
          val uri = base
            .copy(path = (base.path: Path) ++ Path(basePath) ++ Path(queryId.id))
            .withQuery(Uri.Query("from" -> pagination.from.toString, "size" -> pagination.size.toString))
          redirect(uri, StatusCodes.PermanentRedirect)
        }
      }
    } ~
      (get & path(JavaUUID) & pathEndOrSingleSlash & paginated) { (uuid, pagination) =>
        authenticateCaller.apply { implicit caller =>
          onSuccess(queries.fetch(QueryId(uuid.toString))) {
            case Some(Query(_, path, body)) =>
              tracing.trace(s"search${body.resource}") {
                import idsToEntities._
                def buildFromPath[Id, Entity](queries: FilterQueries[Future, Id],
                                              contextId: Boolean = false)(implicit C: ConfiguredQualifier[Id],
                                                                          R: Encoder[UnscoredQueryResult[Id]],
                                                                          S: Encoder[ScoredQueryResult[Id]],
                                                                          Re: Encoder[UnscoredQueryResult[Entity]],
                                                                          Se: Encoder[ScoredQueryResult[Entity]],
                                                                          schemaNameExpr: SchemaNameFilterExpr[Id],
                                                                          idToEntity: IdToEntityRetrieval[Id, Entity],
                                                                          aclRestriction: ResourceRestrictionExpr[Id],
                                                                          L: Encoder[Links]) = {
                  path.segments match {
                    case (orgId :: domainName :: schemaName :: version :: Nil) =>
                      Version(version) match {
                        case None =>
                          exceptionHandler(CommandRejected(IllegalVersionFormat("Illegal version format")))
                        case Some(v) =>
                          val id = SchemaId(DomainId(OrgId(orgId), domainName), schemaName, v)
                          getAcls(Path(id.domainId.show)).apply { implicit acls =>
                            queries.list(id, body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                          }
                      }

                    case (orgId :: domainName :: name :: Nil) =>
                      if (contextId) {
                        val id = ContextName(DomainId(OrgId(orgId), domainName), name)
                        getAcls(Path(id.domainId.show)).apply { implicit acls =>
                          queries.list(id, body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                        }
                      } else {
                        val id = SchemaName(DomainId(OrgId(orgId), domainName), name)
                        getAcls(Path(id.domainId.show)).apply { implicit acls =>
                          queries.list(id, body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                        }
                      }

                    case (orgId :: domainName :: Nil) =>
                      val id = DomainId(OrgId(orgId), domainName)
                      getAcls(Path(id.show)).apply { implicit acls =>
                        queries.list(id, body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                      }

                    case (orgId :: Nil) =>
                      val id = OrgId(orgId)
                      getAcls(id.show / "*").apply { implicit acls =>
                        queries.list(id, body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                      }

                    case Nil =>
                      getAcls("*" / "*").apply { implicit acls =>
                        queries.list(body, pagination).buildResponse(body.fields, base, prefixes, pagination)
                      }

                    case _ => complete(StatusCodes.NotFound)
                  }
                }

                body.resource match {
                  case QueryResource.Organizations =>
                    val enc = new OrgCustomEncoders(base, prefixes)
                    import enc._
                    buildFromPath[OrgId, Organization](FilterQueries[Future, OrgId](client))

                  case QueryResource.Domains =>
                    val enc = new DomainCustomEncoders(base, prefixes)
                    import enc._
                    buildFromPath[DomainId, Domain](FilterQueries[Future, DomainId](client))

                  case QueryResource.Schemas =>
                    val enc = new SchemaCustomEncoders(base, prefixes)
                    import enc._
                    buildFromPath[SchemaId, Schema](FilterQueries[Future, SchemaId](client))

                  case QueryResource.Contexts =>
                    val enc = new ContextCustomEncoders(base, prefixes)
                    import enc._
                    buildFromPath[ContextId, Context](FilterQueries[Future, ContextId](client), contextId = true)

                  case QueryResource.Instances =>
                    val enc = new InstanceCustomEncoders(base, prefixes)
                    import enc._
                    buildFromPath[InstanceId, Instance](FilterQueries[Future, InstanceId](client))

                }
              }
            case None =>
              complete(StatusCodes.NotFound)
          }
        }
      }
}

object QueryRoutes {

  def extractResourcePath: Directive1[Path] = extractUnmatchedPath.map(toInternal)

  /**
    * Constructs a new ''QueryRoutes'' instance that defines the http routes specific to queries.
    *
    * @param queries       the bundle operations for queries
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param idsToEntities the bundle of mappings between ids and entities
    * @param base          the service public uri + prefix
    * @return a new ''QueryRoutes'' instance
    */
  final def apply(queries: Queries[Future],
                  client: SparqlClient[Future],
                  querySettings: QuerySettings,
                  idsToEntities: GroupedIdsToEntityRetrieval,
                  base: Uri)(implicit
                             contexts: Contexts[Future],
                             ec: ExecutionContext,
                             iamClient: IamClient[Future],
                             filteringSettings: FilteringSettings,
                             orderedKeys: OrderedKeys,
                             prefixes: PrefixUris,
                             tracing: TracingDirectives): QueryRoutes = {
    implicit val qs: QuerySettings                = querySettings
    implicit val queryClient: SparqlQuery[Future] = SparqlQuery[Future](client)
    new QueryRoutes(queries, idsToEntities, base)
  }

}
