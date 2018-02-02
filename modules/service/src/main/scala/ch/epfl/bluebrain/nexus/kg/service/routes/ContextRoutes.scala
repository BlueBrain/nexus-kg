package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{UntypedHttpClient, withAkkaUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResults, SortList}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection.CannotUnpublishContext
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.query.contexts.ContextsElasticQueries
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.Publish
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.ContextCustomEncoders
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.IdToEntityRetrieval._
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import kamon.akka.http.KamonTraceDirectives.operationName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for context specific functionality
  *
  * @param contextQueries query builder for contexts
  * @param base           the service public uri + prefix
  * @param prefixes       the service context URIs
  * @param iamClient      IAM client
  * @param ec             execution context
  * @param clock          the clock used to issue instants
  */
class ContextRoutes(contextQueries: FilterQueries[Future, ContextId],
                    contextsElasticQueries: ContextsElasticQueries[Future],
                    base: Uri)(implicit
                               querySettings: QuerySettings,
                               contexts: Contexts[Future],
                               fs: FilteringSettings,
                               iamClient: IamClient[Future],
                               ec: ExecutionContext,
                               clock: Clock,
                               orderedKeys: OrderedKeys,
                               prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts)
    with QueryDirectives {

  private implicit val contextEncoders: ContextCustomEncoders = new ContextCustomEncoders(base, prefixes)
  import contextEncoders._

  private val exceptionHandler = ExceptionHandling.exceptionHandler(prefixes.ErrorContext)

  override protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractContextId { contextId =>
      pathEndOrSingleSlash {
        (put & entity(as[Json])) { json =>
          (authenticateCaller & authorizeResource(contextId, Write)) { implicit caller =>
            parameter('rev.as[Long].?) {
              case Some(rev) =>
                operationName("updateContext") {
                  onSuccess(contexts.update(contextId, rev, json)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              case None =>
                operationName("createContext") {
                  onSuccess(contexts.create(contextId, json)) { ref =>
                    complete(StatusCodes.Created -> ref)

                  }
                }
            }
          }

        } ~
          (delete & parameter('rev.as[Long])) { rev =>
            (authenticateCaller & authorizeResource(contextId, Write)) { implicit caller =>
              operationName("deprecateContext") {
                onSuccess(contexts.deprecate(contextId, rev)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          }
      } ~
        path("config") {
          (pathEndOrSingleSlash & patch & entity(as[ContextConfig]) & parameter('rev.as[Long])) { (cfg, rev) =>
            (authenticateCaller & authorizeResource(contextId, Publish)) { implicit caller =>
              if (cfg.published) {
                operationName("publishSchema") {
                  onSuccess(contexts.publish(contextId, rev)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              } else exceptionHandler(CommandRejected(CannotUnpublishContext))
            }
          }
        }
    }

  override protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route = {
    import contextEncoders.marshallerHttp
    extractContextId { contextId =>
      (pathEndOrSingleSlash & get & authorizeResource(contextId, Read) & format) { format =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            operationName("getContextRevision") {
              onSuccess(contexts.fetch(contextId, rev)) {
                case Some(context) => formatOutput(context, format)
                case None          => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            operationName("getContext") {
              onSuccess(contexts.fetch(contextId)) {
                case Some(context) => formatOutput(context, format)
                case None          => complete(StatusCodes.NotFound)
              }
            }
        }
      }
    }
  }

  override protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paramsToQuery) { (pagination, query) =>
      operationName("searchContexts") {
        implicit val _ = contextIdToEntityRetrieval(contexts)
        (pathEndOrSingleSlash & authenticateCaller) { implicit caller =>
          query.sort match {
            case SortList.Empty =>
              contextsElasticQueries
                .list(pagination, query.deprecated, query.published)
                .buildResponse(query.fields, base, prefixes, pagination)
            case _ =>
              contextQueries.list(query, pagination).buildResponse(query.fields, base, prefixes, pagination)
          }
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            authenticateCaller.apply { implicit caller =>
              query.sort match {
                case SortList.Empty =>
                  contextsElasticQueries
                    .list(pagination, orgId, query.deprecated, query.published)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  contextQueries
                    .list(orgId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractDomainId & pathEndOrSingleSlash) { domainId =>
            authenticateCaller.apply { implicit caller =>
              query.sort match {
                case SortList.Empty =>
                  contextsElasticQueries
                    .list(pagination, domainId, query.deprecated, query.published)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  contextQueries
                    .list(domainId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractContextName & pathEndOrSingleSlash) { contextName =>
            authenticateCaller.apply { implicit caller =>
              query.sort match {
                case SortList.Empty =>
                  contextsElasticQueries
                    .list(pagination, contextName, query.deprecated, query.published)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  contextQueries
                    .list(contextName, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          }
      }
    }

  def routes: Route = combinedRoutesFor("contexts")

}

object ContextRoutes {

  /**
    * Constructs a new ''ContextRoutes'' instance that defines the the http routes specific to contexts.
    *
    * @param base      the service public uri + prefix
    * @param prefixes  the service context URIs
    * @param iamClient IAM client
    * @param ec        execution context
    * @param clock     the clock used to issue instants
    * @return a new ''ContextRoutes'' instance
    */
  final def apply(client: SparqlClient[Future],
                  elasticClient: ElasticClient[Future],
                  elasticSettings: ElasticIndexingSettings,
                  querySettings: QuerySettings,
                  base: Uri)(implicit iamClient: IamClient[Future],
                             contexts: Contexts[Future],
                             filteringSettings: FilteringSettings,
                             ec: ExecutionContext,
                             mt: Materializer,
                             cl: UntypedHttpClient[Future],
                             clock: Clock,
                             orderedKeys: OrderedKeys,
                             prefixes: PrefixUris): ContextRoutes = {

    implicit val qs: QuerySettings = querySettings
    val contextQueries             = FilterQueries[Future, ContextId](SparqlQuery[Future](client))

    implicit val contextIdQualifier: ConfiguredQualifier[ContextId] =
      Qualifier.configured[ContextId](elasticSettings.base)
    implicit val D: Decoder[QueryResults[ContextId]]                   = ElasticDecoder[ContextId]
    implicit val rsSearch: HttpClient[Future, QueryResults[ContextId]] = withAkkaUnmarshaller[QueryResults[ContextId]]
    val contextsElasticQueries                                         = ContextsElasticQueries(elasticClient, elasticSettings)

    new ContextRoutes(contextQueries, contextsElasticQueries, base)
  }

  /**
    * Local context config definition that models a resource for context state change operations
    *
    * @param published whether the context should be published or un-published
    */
  final case class ContextConfig(published: Boolean)

}
