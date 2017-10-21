package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.auth.types.AccessControlList
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization, Organizations}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.directives.PathDirectives.{extractResourceId, of}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.ResourceAccess.{IamUri, check}
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}
import ch.epfl.bluebrain.nexus.kg.auth.types.Permission._

/**
  * Http route definitions for organization specific functionality.
  *
  * @param orgs              the organization operation bundle
  * @param orgQueries        query builder for organizations
  * @param base              the service public uri + prefix
  */
final class OrganizationRoutes(orgs: Organizations[Future], orgQueries: FilterQueries[Future, OrgId], base: Uri)(
    implicit querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    cl: HttpClient[Future, AccessControlList],
    iamUri: IamUri,
    ec: ExecutionContext)
    extends DefaultRouteHandling {

  private val encoders = new OrgCustomEncoders(base)

  import encoders._

  protected def searchRoutes(cred: OAuth2BearerToken): Route =
    (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
      traceName("searchOrganizations") {
        val filter =
          filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
        orgQueries
          .list(filter, pagination, termOpt)
          .buildResponse(base, pagination)
      }
    }

  protected def resourceRoutes(cred: OAuth2BearerToken): Route =
    (extractResourceId[OrgId](2, of[OrgId]) & pathEndOrSingleSlash) { orgId =>
      (put & entity(as[Json]) & authorizeAsync(check(cred, orgId, Write))) { json =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            traceName("updateOrganization") {
              onSuccess(orgs.update(orgId, rev, json)) { ref =>
                complete(StatusCodes.OK -> ref)
              }
            }
          case None =>
            traceName("createOrganization") {
              onSuccess(orgs.create(orgId, json)) { ref =>
                complete(StatusCodes.Created -> ref)
              }
            }
        }
      } ~
        (get & authorizeAsync(check(cred, orgId, Read))) {
          traceName("getOrganization") {
            onSuccess(orgs.fetch(orgId)) {
              case Some(org) => complete(org)
              case None      => complete(StatusCodes.NotFound)
            }
          }
        } ~
        (delete & authorizeAsync(check(cred, orgId, Write))) {
          parameter('rev.as[Long]) { rev =>
            traceName("deprecateOrganization") {
              onSuccess(orgs.deprecate(orgId, rev)) { ref =>
                complete(StatusCodes.OK -> ref)
              }
            }
          }
        }
    }

  def routes: Route = combinedRoutesFor("organizations")
}

object OrganizationRoutes {

  /**
    * Constructs a new ''OrganizationRoutes'' instance that defines the http routes specific to organizations.
    *
    * @param orgs          the organization operation bundle
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param base          the service public uri + prefix
    * @return a new ''OrganizationRoutes'' instance
    */
  final def apply(orgs: Organizations[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit
      ec: ExecutionContext,
      mt: ActorMaterializer,
      baseClient: UntypedHttpClient[Future],
      filteringSettings: FilteringSettings,
      iamUri: IamUri): OrganizationRoutes = {

    implicit val qs: QuerySettings = querySettings
    val orgQueries =
      FilterQueries[Future, OrgId](SparqlQuery[Future](client), querySettings)
    implicit val cl = HttpClient.withAkkaUnmarshaller[AccessControlList]
    new OrganizationRoutes(orgs, orgQueries, base)
  }
}

class OrgCustomEncoders(base: Uri) extends RoutesEncoder[OrgId, OrgRef](base) {

  implicit val orgEncoder: Encoder[Organization] =
    Encoder.encodeJson.contramap { org =>
      val meta = refEncoder
        .apply(OrgRef(org.id, org.rev))
        .deepMerge(
          Json.obj(
            "deprecated" -> Json.fromBoolean(org.deprecated)
          ))
      org.value.deepMerge(meta)
    }
}
