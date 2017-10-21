package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives.{authorizeAsync, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.auth.types.AccessControlList
import ch.epfl.bluebrain.nexus.kg.auth.types.Permission._
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.DomainRoutes.DomainDescription
import ch.epfl.bluebrain.nexus.kg.service.routes.ResourceAccess.{IamUri, check}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for domain specific functionality.
  *
  * @param domains           the domain operation bundle
  * @param domainQueries     query builder for domains
  * @param base              the service public uri + prefix
  */
final class DomainRoutes(domains: Domains[Future], domainQueries: FilterQueries[Future, DomainId], base: Uri)(
    implicit querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    cl: HttpClient[Future, AccessControlList],
    iamUri: IamUri,
    ec: ExecutionContext)
    extends DefaultRouteHandling {

  private val encoders = new DomainCustomEncoders(base)
  import encoders._

  protected def searchRoutes(cred: OAuth2BearerToken): Route =
    (get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
      val filter =
        filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
      traceName("searchDomains") {
        pathEndOrSingleSlash {
          domainQueries
            .list(filter, pagination, termOpt)
            .buildResponse(base, pagination)
        } ~
          (extractResourceId[OrgId](1, of[OrgId]) & pathEndOrSingleSlash) { orgId =>
            domainQueries
              .list(orgId, filter, pagination, termOpt)
              .buildResponse(base, pagination)
          }
      }
    }

  protected def resourceRoutes(cred: OAuth2BearerToken): Route =
    (extractResourceId[DomainId](2, of[DomainId]) & pathEndOrSingleSlash) { domainId =>
      (put & entity(as[DomainDescription]) & authorizeAsync(check(cred, domainId, Write))) { desc =>
        traceName("createDomain") {
          onSuccess(domains.create(domainId, desc.description)) { ref =>
            complete(StatusCodes.Created -> ref)
          }
        }
      } ~
        (get & authorizeAsync(check(cred, domainId, Read))) {
          traceName("getDomain") {
            onSuccess(domains.fetch(domainId)) {
              case Some(domain) => complete(domain)
              case None         => complete(StatusCodes.NotFound)
            }
          }
        } ~
        (delete & authorizeAsync(check(cred, domainId, Write))) {
          parameter('rev.as[Long]) { rev =>
            traceName("deprecateDomain") {
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
      ec: ExecutionContext,
      mt: ActorMaterializer,
      baseClient: UntypedHttpClient[Future],
      filteringSettings: FilteringSettings,
      iamUri: IamUri): DomainRoutes = {
    implicit val qs: QuerySettings = querySettings
    val domainQueries              = FilterQueries[Future, DomainId](SparqlQuery[Future](client), querySettings)
    implicit val cl                = HttpClient.withAkkaUnmarshaller[AccessControlList]
    new DomainRoutes(domains, domainQueries, base)
  }
}

class DomainCustomEncoders(base: Uri) extends RoutesEncoder[DomainId, DomainRef](base) {

  implicit def domainEncoder: Encoder[Domain] = Encoder.encodeJson.contramap { domain =>
    refEncoder
      .apply(DomainRef(domain.id, domain.rev))
      .deepMerge(
        Json.obj(
          "deprecated"  -> Json.fromBoolean(domain.deprecated),
          "description" -> Json.fromString(domain.description)
        ))
  }
}
