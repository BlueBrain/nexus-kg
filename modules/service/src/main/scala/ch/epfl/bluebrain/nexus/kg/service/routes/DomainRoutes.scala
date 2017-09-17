package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.query.FilterQueries
import ch.epfl.bluebrain.nexus.kg.service.routes.DomainRoutes.DomainDescription
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for domain specific functionality.
  *
  * @param domains       the domain operation bundle
  * @param querySettings query parameters form settings
  * @param queryBuilder  query builder for domains
  * @param base          the service public uri + prefix
  */
final class DomainRoutes(domains: Domains[Future], querySettings: QuerySettings, queryBuilder: FilterQueries[DomainId], base: Uri) {

  private val encoders = new DomainCustomEncoders(base)
  import encoders._, queryBuilder._

  private val pagination = querySettings.pagination

  def routes: Route = handleExceptions(ExceptionHandling.exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix("organizations" / Segment / "domains") { orgIdString =>
        val orgId = OrgId(orgIdString)
        pathEndOrSingleSlash {
          (get & parameter('from.as[Int] ? pagination.from) & parameter('size.as[Int] ? pagination.size) & parameter('deprecated.as[Boolean].?)) { (from, size, deprecated) =>
            traceName("listDomains") {
              queryBuilder.listingQuery(orgId, deprecated, None, Pagination(from, size)).response
            }
          }
        } ~
        pathPrefix(Segment) { id =>
          val domainId = DomainId(orgId, id)
          pathEnd {
            (put & entity(as[DomainDescription])) { desc =>
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
        }
      }
    }
  }
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
  final def apply(domains: Domains[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(implicit
    ec: ExecutionContext): DomainRoutes = {
    val filterQueries = new FilterQueries[DomainId](SparqlQuery[Future](client), querySettings, base)
    new DomainRoutes(domains, querySettings, filterQueries, base)
  }
}

private class DomainCustomEncoders(base: Uri) extends RoutesEncoder[DomainId, DomainRef](base){

  implicit def domainEncoder: Encoder[Domain] = Encoder.encodeJson.contramap { domain =>
    refEncoder.apply(DomainRef(domain.id, domain.rev)).deepMerge(Json.obj(
      "deprecated" -> Json.fromBoolean(domain.deprecated),
      "description" -> Json.fromString(domain.description)
    ))
  }
}