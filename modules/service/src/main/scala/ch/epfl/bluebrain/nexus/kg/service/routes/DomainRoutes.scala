package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{UntypedHttpClient, withAkkaUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.http.{ContextUri, HttpClient}
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResults, SortList}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.query.domains.DomainsElasticQueries
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.{authenticateCaller, _}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.routes.DomainRoutes.DomainDescription
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.DomainCustomEncoders
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.IdToEntityRetrieval._
import io.circe.Decoder
import io.circe.generic.auto._
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for domain specific functionality.
  *
  * @param domains               the domain operation bundle
  * @param domainQueries         query builder for domains
  * @param domainsElasticQueries Elastic search client for domains
  * @param base                  the service public uri + prefix
  */
final class DomainRoutes(domains: Domains[Future],
                         domainQueries: FilterQueries[Future, DomainId],
                         domainsElasticQueries: DomainsElasticQueries[Future],
                         base: Uri)(implicit contexts: Contexts[Future],
                                    querySettings: QuerySettings,
                                    filteringSettings: FilteringSettings,
                                    iamClient: IamClient[Future],
                                    ec: ExecutionContext,
                                    clock: Clock,
                                    orderedKeys: OrderedKeys,
                                    prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts) {

  private implicit val coreContext: ContextUri        = prefixes.CoreContext
  private implicit val encoders: DomainCustomEncoders = new DomainCustomEncoders(base, prefixes)
  import encoders._

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paramsToQuery) { (pagination, query) =>
      implicit val _ = domainIdToEntityRetrieval(domains)
      operationName("searchDomains") {
        (pathEndOrSingleSlash & getAcls("*" / "*")) { implicit acls =>
          (query.filter, query.q, query.sort) match {
            case (Filter.Empty, None, SortList.Empty) =>
              domainsElasticQueries
                .list(pagination, query.deprecated, None)
                .buildResponse(query.fields, base, prefixes, pagination)
            case _ =>
              domainQueries
                .list(query, pagination)
                .buildResponse(query.fields, base, prefixes, pagination)
          }
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            getAcls(orgId.show / "*").apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  domainsElasticQueries
                    .list(pagination, orgId, query.deprecated, None)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  domainQueries
                    .list(orgId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          }
      }
    }

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (extractDomainId & pathEndOrSingleSlash) { domainId =>
      (get & authorizeResource(domainId, Read) & format) { format =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            operationName("getDomainRevision") {
              onSuccess(domains.fetch(domainId, rev)) {
                case Some(domain) => formatOutput(domain, format)
                case None         => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            operationName("getDomain") {
              onSuccess(domains.fetch(domainId)) {
                case Some(domain) => formatOutput(domain, format)
                case None         => complete(StatusCodes.NotFound)
              }
            }
        }
      }
    }

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (extractDomainId & pathEndOrSingleSlash) { domainId =>
      (put & entity(as[DomainDescription])) { desc =>
        (authenticateCaller & authorizeResource(domainId, Write)) { implicit caller =>
          operationName("createDomain") {
            onSuccess(domains.create(domainId, desc.description)) { ref =>
              complete(StatusCodes.Created -> ref)
            }
          }
        }
      } ~
        (delete & parameter('rev.as[Long])) { rev =>
          (authenticateCaller & authorizeResource(domainId, Write)) { implicit caller =>
            operationName("deprecateDomain") {
              onSuccess(domains.deprecate(domainId, rev)) { ref =>
                complete(StatusCodes.OK -> ref)
              }
            }
          }
        }
    }

  def routes: Route = combinedRoutesFor("domains")

}

object DomainRoutes {

  /**
    * Local data type that wraps a textual description of a domain.
    *
    * @param description a domain description
    */
  final case class DomainDescription(description: String)

  /**
    * Constructs a new ''DomainRoutes'' instance that defines the http routes specific to domains.
    *
    * @param domains         the domain operation bundle
    * @param client          the sparql client
    * @param elasticClient   Elastic Search client
    * @param elasticSettings Elastic Search settings
    * @param querySettings   query parameters form settings
    * @param base            the service public uri + prefix
    * @return a new ''DomainRoutes'' instance
    */
  final def apply(domains: Domains[Future],
                  client: SparqlClient[Future],
                  elasticClient: ElasticClient[Future],
                  elasticSettings: ElasticIndexingSettings,
                  querySettings: QuerySettings,
                  base: Uri)(implicit
                             contexts: Contexts[Future],
                             ec: ExecutionContext,
                             mt: Materializer,
                             iamClient: IamClient[Future],
                             cl: UntypedHttpClient[Future],
                             filteringSettings: FilteringSettings,
                             clock: Clock,
                             orderedKeys: OrderedKeys,
                             prefixes: PrefixUris): DomainRoutes = {
    implicit val qs: QuerySettings = querySettings
    val domainQueries              = FilterQueries[Future, DomainId](SparqlQuery[Future](client))

    implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](elasticSettings.base)
    implicit val D: Decoder[QueryResults[DomainId]]                   = ElasticDecoder[DomainId]
    implicit val rsSearch: HttpClient[Future, QueryResults[DomainId]] = withAkkaUnmarshaller[QueryResults[DomainId]]
    val domainsElasticQueries                                         = DomainsElasticQueries(elasticClient, elasticSettings)
    new DomainRoutes(domains, domainQueries, domainsElasticQueries, base)
  }
}
