package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization, Organizations}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives.traceName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for organization specific functionality.
  *
  * @param orgs              the organization operation bundle
  * @param orgQueries        query builder for organizations
  * @param base              the service public uri + prefix
  * @param prefixes          the service context URIs
  */
final class OrganizationRoutes(orgs: Organizations[Future],
                               orgQueries: FilterQueries[Future, OrgId],
                               base: Uri,
                               prefixes: PrefixUris)(implicit querySettings: QuerySettings,
                                                     filteringSettings: FilteringSettings,
                                                     iamClient: IamClient[Future],
                                                     ec: ExecutionContext,
                                                     clock: Clock,
                                                     orderedKeys: OrderedKeys)
    extends DefaultRouteHandling {

  private implicit val _                           = (entity: Organization) => entity.id
  private implicit val encoders: OrgCustomEncoders = new OrgCustomEncoders(base, prefixes)
  import encoders._

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt, fields) =>
      (traceName("searchOrganizations") & authenticateCaller) { implicit caller =>
        val filter =
          filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
        implicit val _ = (id: OrgId) => orgs.fetch(id)
        orgQueries
          .list(filter, pagination, termOpt)
          .buildResponse(fields, base, pagination)
      }
    }

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (extractOrgId & pathEndOrSingleSlash) { orgId =>
      (get & authorizeResource(orgId, Read)) {
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            traceName("getOrganizationRevision") {
              onSuccess(orgs.fetch(orgId, rev)) {
                case Some(org) => complete(org)
                case None      => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            traceName("getOrganization") {
              onSuccess(orgs.fetch(orgId)) {
                case Some(org) => complete(org)
                case None      => complete(StatusCodes.NotFound)
              }
            }
        }
      }
    }

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (extractOrgId & pathEndOrSingleSlash) { orgId =>
      (put & entity(as[Json])) { json =>
        (authenticateCaller & authorizeResource(orgId, Write)) { implicit caller =>
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
        }
      } ~
        (delete & parameter('rev.as[Long])) { rev =>
          (authenticateCaller & authorizeResource(orgId, Write)) { implicit caller =>
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
    * @param prefixes      the service context URIs
    * @return a new ''OrganizationRoutes'' instance
    */
  final def apply(orgs: Organizations[Future],
                  client: SparqlClient[Future],
                  querySettings: QuerySettings,
                  base: Uri,
                  prefixes: PrefixUris)(implicit
                                        ec: ExecutionContext,
                                        iamClient: IamClient[Future],
                                        filteringSettings: FilteringSettings,
                                        clock: Clock,
                                        orderedKeys: OrderedKeys): OrganizationRoutes = {

    implicit val qs: QuerySettings = querySettings
    val orgQueries =
      FilterQueries[Future, OrgId](SparqlQuery[Future](client), querySettings)
    new OrganizationRoutes(orgs, orgQueries, base, prefixes)
  }
}

class OrgCustomEncoders(base: Uri, prefixes: PrefixUris)(implicit E: Organization => OrgId)
    extends RoutesEncoder[OrgId, OrgRef, Organization](base, prefixes) {

  implicit val orgRefEncoder: Encoder[OrgRef] = refEncoder.mapJson(_.addCoreContext)

  implicit val orgEncoder: Encoder[Organization] = Encoder.encodeJson.contramap { org =>
    val meta = refEncoder
      .apply(OrgRef(org.id, org.rev))
      .deepMerge(idWithLinksEncoder(org.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(org.deprecated)
        ))
    org.value.deepMerge(meta).addCoreContext
  }
}
