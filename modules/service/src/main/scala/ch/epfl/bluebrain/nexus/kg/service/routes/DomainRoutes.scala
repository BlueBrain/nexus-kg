package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef, Domains}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives.{authenticateCaller, _}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.DomainRoutes.DomainDescription
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for domain specific functionality.
  *
  * @param domains           the domain operation bundle
  * @param domainQueries     query builder for domains
  * @param base              the service public uri + prefix
  */
final class DomainRoutes(domains: Domains[Future], domainQueries: FilterQueries[Future, DomainId], base: Uri)(
    implicit contexts: Contexts[Future],
    querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext,
    clock: Clock,
    orderedKeys: OrderedKeys,
    prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts) {

  private implicit val _                              = (entity: Domain) => entity.id
  private implicit val coreContext: ContextUri        = prefixes.CoreContext
  private implicit val encoders: DomainCustomEncoders = new DomainCustomEncoders(base, prefixes)
  import encoders._

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt, fields, sort) =>
      val filter =
        filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
      implicit val _ = (id: DomainId) => domains.fetch(id)
      operationName("searchDomains") {
        (pathEndOrSingleSlash & authenticateCaller) { implicit caller =>
          domainQueries
            .list(filter, pagination, termOpt, sort)
            .buildResponse(fields, base, prefixes, pagination)
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            authenticateCaller.apply { implicit caller =>
              domainQueries
                .list(orgId, filter, pagination, termOpt, sort)
                .buildResponse(fields, base, prefixes, pagination)
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
    * @param domains       the domain operation bundle
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param base          the service public uri + prefix
    * @return a new ''DomainRoutes'' instance
    */
  final def apply(domains: Domains[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit
      contexts: Contexts[Future],
      ec: ExecutionContext,
      iamClient: IamClient[Future],
      filteringSettings: FilteringSettings,
      clock: Clock,
      orderedKeys: OrderedKeys,
      prefixes: PrefixUris): DomainRoutes = {
    implicit val qs: QuerySettings = querySettings
    val domainQueries              = FilterQueries[Future, DomainId](SparqlQuery[Future](client), querySettings)
    new DomainRoutes(domains, domainQueries, base)
  }
}

class DomainCustomEncoders(base: Uri, prefixes: PrefixUris)(implicit E: Domain => DomainId)
    extends RoutesEncoder[DomainId, DomainRef, Domain](base, prefixes) {

  implicit val domainRefEncoder: Encoder[DomainRef] = refEncoder

  implicit def domainEncoder: Encoder[Domain] = Encoder.encodeJson.contramap { domain =>
    refEncoder
      .apply(DomainRef(domain.id, domain.rev))
      .deepMerge(idWithLinksEncoder(domain.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated  -> Json.fromBoolean(domain.deprecated),
          JsonLDKeys.nxvDescription -> Json.fromString(domain.description)
        ))
  }
}
