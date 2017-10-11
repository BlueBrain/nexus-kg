package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.{
  Domain,
  DomainId,
  DomainRef,
  Domains
}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.DomainRoutesDeprecated.DomainDescriptionDeprecated
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  *
  * Http route definitions for domain specific functionality.
  *
  * @param domains           the domain operation bundle
  * @param domainQueries     query builder for domains
  * @param base              the service public uri + prefix
  * @param querySettings     query parameters from settings
  * @param filteringSettings filtering parameters from settings
  */
@deprecated("Backwards compatible API endpoints", "0.6.2")
final class DomainRoutesDeprecated(
    domains: Domains[Future],
    domainQueries: FilterQueries[Future, DomainId],
    base: Uri)(implicit querySettings: QuerySettings,
               filteringSettings: FilteringSettings)
    extends DefaultRouteHandling {

  private val encoders = new DomainCustomEncodersDeprecated(base)
  import encoders._

  protected def searchRoutes: Route =
    pathPrefix(Segment / "domains") { orgIdString =>
      val orgId = OrgId(orgIdString)
      (pathEndOrSingleSlash & get & searchQueryParams) {
        (pagination, filterOpt, termOpt, deprecatedOpt) =>
          traceName("searchDomains") {
            val filter =
              filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
            domainQueries
              .list(orgId, filter, pagination, termOpt)
              .buildResponse(base, pagination)
          }
      }
    }

  protected def resourceRoutes: Route =
    (pathPrefix(Segment / "domains" / Segment) & pathEndOrSingleSlash) {
      (orgId, id) =>
        val domainId = DomainId(OrgId(orgId), id)
        (put & entity(as[DomainDescriptionDeprecated])) { desc =>
          traceName("createDomain") {
            onSuccess(domains.create(domainId, desc.description)) { ref =>
              complete(StatusCodes.Created -> ref)
            }
          }
        } ~
          get {
            traceName("getDomain") {
              onSuccess(domains.fetch(domainId)) {
                case Some(domain) => complete(domain)
                case None         => complete(StatusCodes.NotFound)
              }
            }
          } ~
          delete {
            parameter('rev.as[Long]) { rev =>
              traceName("deprecateDomain") {
                onSuccess(domains.deprecate(domainId, rev)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          }
    }

  def routes: Route = combinedRoutesFor("organizations")

}

object DomainRoutesDeprecated {

  /**
    * Local data type that wraps a textual description of a domain.
    *
    * @param description a domain description
    */
  final case class DomainDescriptionDeprecated(description: String)

  /**
    * Constructs a new ''DomainRoutesDeprecated'' instance that defines the http routes specific to domains.
    *
    * @param domains       the domain operation bundle
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param base          the service public uri + prefix
    * @return a new ''DomainRoutesDeprecated'' instance
    */
  final def apply(domains: Domains[Future],
                  client: SparqlClient[Future],
                  querySettings: QuerySettings,
                  base: Uri)(
      implicit
      ec: ExecutionContext,
      filteringSettings: FilteringSettings): DomainRoutesDeprecated = {
    implicit val qs: QuerySettings = querySettings
    val domainQueries = FilterQueries[Future, DomainId](
      SparqlQuery[Future](client),
      querySettings)
    new DomainRoutesDeprecated(domains, domainQueries, base)
  }
}

class DomainCustomEncodersDeprecated(base: Uri)
    extends RoutesEncoder[DomainId, DomainRef](base) {

  implicit def domainEncoder: Encoder[Domain] = Encoder.encodeJson.contramap {
    domain =>
      refEncoder
        .apply(DomainRef(domain.id, domain.rev))
        .deepMerge(
          Json.obj(
            "deprecated" -> Json.fromBoolean(domain.deprecated),
            "description" -> Json.fromString(domain.description)
          ))
  }
}
