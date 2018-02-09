package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{UntypedHttpClient, withAkkaUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.http.{ContextUri, HttpClient}
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResults, SortList}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceId, Instances}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.query.instances.InstancesElasticQueries
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.IdToEntityRetrieval._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.InstanceCustomEncoders
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import kamon.akka.http.KamonTraceDirectives.operationName
import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for instance specific functionality.
  *
  * @param instances               the instances operation bundle
  * @param instanceQueries         query builder for schemas
  * @param instancesElasticQueries Elastic search client for instances
  * @param base                    the service public uri + prefix
  * @param prefixes                the service context URIs
  */
class InstanceRoutes(instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
                     instanceQueries: FilterQueries[Future, InstanceId],
                     instancesElasticQueries: InstancesElasticQueries[Future],
                     base: Uri)(implicit
                                contexts: Contexts[Future],
                                querySettings: QuerySettings,
                                filteringSettings: FilteringSettings,
                                iamClient: IamClient[Future],
                                ec: ExecutionContext,
                                clock: Clock,
                                orderedKeys: OrderedKeys,
                                prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts) {

  private implicit val encoders: InstanceCustomEncoders = new InstanceCustomEncoders(base, prefixes)
  private implicit val coreContext: ContextUri          = prefixes.CoreContext
  import encoders._

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paramsToQuery) { (pagination, query) =>
      implicit val _ = instanceIdToEntityRetrieval(instances)
      operationName("searchInstances") {
        (pathEndOrSingleSlash & getAcls("*" / "*")) { implicit acls =>
          (query.filter, query.q, query.sort) match {
            case (Filter.Empty, None, SortList.Empty) =>
              instancesElasticQueries
                .list(pagination, query.deprecated, None)
                .buildResponse(query.fields, base, prefixes, pagination)
            case _ =>
              instanceQueries.list(query, pagination).buildResponse(query.fields, base, prefixes, pagination)
          }
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            getAcls(orgId.show / "*").apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  instancesElasticQueries
                    .list(pagination, orgId, query.deprecated, None)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  instanceQueries
                    .list(orgId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractDomainId & pathEndOrSingleSlash) { domainId =>
            getAcls(Path(domainId.show)).apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  instancesElasticQueries
                    .list(pagination, domainId, query.deprecated, None)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  instanceQueries
                    .list(domainId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractSchemaName & pathEndOrSingleSlash) { schemaName =>
            (query.filter, query.q, query.sort) match {
              case (Filter.Empty, None, SortList.Empty) =>
                instancesElasticQueries
                  .list(pagination, schemaName, query.deprecated, None)
                  .buildResponse(query.fields, base, prefixes, pagination)
              case _ =>
                getAcls(Path(schemaName.domainId.show)).apply { implicit acls =>
                  instanceQueries
                    .list(schemaName, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
                }

            }
          } ~
          (extractSchemaId & pathEndOrSingleSlash) { schemaId =>
            getAcls(Path(schemaId.domainId.show)).apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  instancesElasticQueries
                    .list(pagination, schemaId, query.deprecated, None)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  instanceQueries
                    .list(schemaId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          extractInstanceId { instanceId =>
            (path("outgoing") & getAcls(Path(instanceId.schemaId.domainId.show))) { implicit acls =>
              instanceQueries
                .outgoing(instanceId, query, pagination)
                .buildResponse(query.fields, base, prefixes, pagination)
            } ~
              (path("incoming") & getAcls(Path(instanceId.schemaId.domainId.show))) { implicit acls =>
                instanceQueries
                  .incoming(instanceId, query, pagination)
                  .buildResponse(query.fields, base, prefixes, pagination)
              }
          }
      }
    }

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractInstanceId { instanceId =>
      (pathEndOrSingleSlash & get & authorizeResource(instanceId, Read) & format) { format =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            operationName("getInstanceRevision") {
              onSuccess(instances.fetch(instanceId, rev)) {
                case Some(instance) => formatOutput(instance, format)
                case None           => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            operationName("getInstance") {
              onSuccess(instances.fetch(instanceId)) {
                case Some(instance) => formatOutput(instance, format)
                case None           => complete(StatusCodes.NotFound)
              }
            }
        }
      } ~
        path("attachment") {
          (pathEndOrSingleSlash & get & authorizeResource(Path(s"${instanceId.show}/attachment"), Read)) {
            parameter('rev.as[Long].?) { revOpt =>
              operationName("getInstanceAttachment") {
                val result = revOpt match {
                  case Some(rev) => instances.fetchAttachment(instanceId, rev)
                  case None      => instances.fetchAttachment(instanceId)
                }
                onSuccess(result) {
                  case Some((info, source)) =>
                    val ct =
                      ContentType.parse(info.mediaType).getOrElse(ContentTypes.`application/octet-stream`)
                    complete(HttpEntity(ct, info.contentSize.value, source))
                  case None =>
                    complete(StatusCodes.NotFound)
                }
              }
            }
          }
        }
    }

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (extractSchemaId & pathEndOrSingleSlash & post) { schemaId =>
      entity(as[Json]) { json =>
        (authenticateCaller & authorizeResource(schemaId, Write)) { implicit caller =>
          operationName("createInstance") {
            onSuccess(instances.create(schemaId, json)) { ref =>
              complete(StatusCodes.Created -> ref)
            }
          }
        }
      }
    } ~
      extractInstanceId { instanceId =>
        pathEndOrSingleSlash {
          (put & entity(as[Json]) & parameter('rev.as[Long])) { (json, rev) =>
            (authenticateCaller & authorizeResource(instanceId, Write)) { implicit caller =>
              operationName("updateInstance") {
                onSuccess(instances.update(instanceId, rev, json)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          } ~
            (delete & parameter('rev.as[Long])) { rev =>
              (authenticateCaller & authorizeResource(instanceId, Write)) { implicit caller =>
                operationName("deprecateInstance") {
                  onSuccess(instances.deprecate(instanceId, rev)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              }
            }
        } ~
          path("attachment") {
            val resource = Path(s"${instanceId.show}/attachment")
            (put & parameter('rev.as[Long])) { rev =>
              (authenticateCaller & authorizeResource(resource, Write)) { implicit caller =>
                fileUpload("file") {
                  case (metadata, byteSource) =>
                    operationName("createInstanceAttachment") {
                      onSuccess(instances
                        .createAttachment(instanceId, rev, metadata.fileName, metadata.contentType.value, byteSource)) {
                        info =>
                          complete(StatusCodes.Created -> info)
                      }
                    }
                }
              }
            } ~
              (delete & parameter('rev.as[Long])) { rev =>
                (authenticateCaller & authorizeResource(resource, Write)) { implicit caller =>
                  operationName("removeInstanceAttachment") {
                    onSuccess(instances.removeAttachment(instanceId, rev)) { ref =>
                      complete(StatusCodes.OK -> ref)
                    }
                  }
                }
              }
          }
      }

  def routes: Route = combinedRoutesFor("data")
}

object InstanceRoutes {

  /**
    * Constructs a new ''InstanceRoutes'' instance that defines the http routes specific to instances.
    *
    * @param instances       the instances operation bundle
    * @param contexts        the context operation bundle
    * @param client          the sparql client
    * @param elasticClient   Elastic Search client
    * @param elasticSettings Elastic Search settings
    * @param querySettings   query parameters form settings
    * @param base            the service public uri + prefix
    * @param prefixes        the service context URIs
    * @return a new ''InstanceRoutes'' instance
    */
  final def apply(instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
                  client: SparqlClient[Future],
                  elasticClient: ElasticClient[Future],
                  elasticSettings: ElasticIndexingSettings,
                  querySettings: QuerySettings,
                  base: Uri)(implicit
                             contexts: Contexts[Future],
                             ec: ExecutionContext,
                             mt: Materializer,
                             cl: UntypedHttpClient[Future],
                             iamClient: IamClient[Future],
                             filteringSettings: FilteringSettings,
                             clock: Clock,
                             orderedKeys: OrderedKeys,
                             prefixes: PrefixUris): InstanceRoutes = {
    implicit val qs: QuerySettings = querySettings
    val instanceQueries            = FilterQueries[Future, InstanceId](SparqlQuery[Future](client))

    implicit val instanceIdQualifier: ConfiguredQualifier[InstanceId] =
      Qualifier.configured[InstanceId](elasticSettings.base)
    implicit val D: Decoder[QueryResults[InstanceId]]                   = ElasticDecoder[InstanceId]
    implicit val rsSearch: HttpClient[Future, QueryResults[InstanceId]] = withAkkaUnmarshaller[QueryResults[InstanceId]]
    val instancesElasticQueries                                         = InstancesElasticQueries(elasticClient, elasticSettings)

    new InstanceRoutes(instances, instanceQueries, instancesElasticQueries, base)
  }

}
