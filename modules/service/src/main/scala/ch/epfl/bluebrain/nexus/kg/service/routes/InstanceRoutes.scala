package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{authorizeAsync, _}
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.auth.types.AccessControlList
import ch.epfl.bluebrain.nexus.kg.auth.types.Permission._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.ResourceAccess.{IamUri, check}
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives.traceName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for instance specific functionality.
  *
  * @param instances         the instances operation bundle
  * @param instanceQueries   query builder for schemas
  * @param base              the service public uri + prefix
  */
class InstanceRoutes(instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
                     instanceQueries: FilterQueries[Future, InstanceId],
                     base: Uri)(implicit querySettings: QuerySettings,
                                filteringSettings: FilteringSettings,
                                cl: HttpClient[Future, AccessControlList],
                                iamUri: IamUri,
                                ec: ExecutionContext)
    extends DefaultRouteHandling {

  private val encoders = new InstanceCustomEncoders(base)

  import encoders._

  protected def searchRoutes(cred: OAuth2BearerToken): Route =
    (get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
      val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
      traceName("searchInstances") {
        pathEndOrSingleSlash {
          instanceQueries.list(filter, pagination, termOpt).buildResponse(base, pagination)
        } ~
          extractAnyResourceId() { id =>
            (pathEndOrSingleSlash & resourceId(id, of[OrgId])) { orgId =>
              instanceQueries.list(orgId, filter, pagination, termOpt).buildResponse(base, pagination)
            } ~
              (pathEndOrSingleSlash & resourceId(id, of[DomainId])) { domainId =>
                instanceQueries.list(domainId, filter, pagination, termOpt).buildResponse(base, pagination)
              } ~
              (pathEndOrSingleSlash & resourceId(id, of[SchemaName])) { schemaName =>
                instanceQueries.list(schemaName, filter, pagination, termOpt).buildResponse(base, pagination)
              } ~
              (pathEndOrSingleSlash & resourceId(id, of[SchemaId])) { schemaId =>
                instanceQueries.list(schemaId, filter, pagination, termOpt).buildResponse(base, pagination)
              } ~
              (resourceId(id, of[InstanceId]) & pathPrefix("outgoing") & pathEndOrSingleSlash) { instanceId =>
                instanceQueries.outgoing(instanceId, filter, pagination, termOpt).buildResponse(base, pagination)
              } ~
              (resourceId(id, of[InstanceId]) & pathPrefix("incoming") & pathEndOrSingleSlash) { instanceId =>
                instanceQueries.incoming(instanceId, filter, pagination, termOpt).buildResponse(base, pagination)
              }
          }
      }
    }

  protected def resourceRoutes(cred: OAuth2BearerToken): Route =
    extractAnyResourceId() { id =>
      (pathEndOrSingleSlash & post & resourceId(id, of[SchemaId])) { schemaId =>
        (entity(as[Json]) & authorizeAsync(check(cred, schemaId, Write))) { json =>
          traceName("createInstance") {
            onSuccess(instances.create(schemaId, json)) { ref =>
              complete(StatusCodes.Created -> ref)
            }
          }
        }
      } ~
        resourceId(id, of[InstanceId]) { instanceId =>
          pathEndOrSingleSlash {
            (put & entity(as[Json]) & parameter('rev.as[Long]) & authorizeAsync(check(cred, instanceId, Write))) {
              (json, rev) =>
                traceName("updateInstance") {
                  onSuccess(instances.update(instanceId, rev, json)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
            } ~
              (get & authorizeAsync(check(cred, instanceId, Read))) {
                parameter('rev.as[Long].?) {
                  case Some(rev) =>
                    traceName("getInstanceRevision") {
                      onSuccess(instances.fetch(instanceId, rev)) {
                        case Some(instance) => complete(StatusCodes.OK -> instance)
                        case None           => complete(StatusCodes.NotFound)
                      }
                    }
                  case None =>
                    traceName("getInstance") {
                      onSuccess(instances.fetch(instanceId)) {
                        case Some(instance) => complete(StatusCodes.OK -> instance)
                        case None           => complete(StatusCodes.NotFound)
                      }
                    }
                }
              } ~
              (delete & parameter('rev.as[Long]) & authorizeAsync(check(cred, instanceId, Write))) { rev =>
                traceName("deprecateInstance") {
                  onSuccess(instances.deprecate(instanceId, rev)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              }
          } ~
            path("attachment") {
              (put & parameter('rev.as[Long]) & authorizeAsync(check(cred, s"$instanceId/attachment", Write))) { rev =>
                fileUpload("file") {
                  case (metadata, byteSource) =>
                    traceName("createInstanceAttachment") {
                      onSuccess(instances
                        .createAttachment(instanceId, rev, metadata.fileName, metadata.contentType.value, byteSource)) {
                        info =>
                          complete(StatusCodes.Created -> info)
                      }
                    }
                }
              } ~
                (delete & parameter('rev.as[Long]) & authorizeAsync(check(cred, s"$instanceId/attachment", Write))) {
                  rev =>
                    traceName("removeInstanceAttachment") {
                      onSuccess(instances.removeAttachment(instanceId, rev)) { ref =>
                        complete(StatusCodes.OK -> ref)
                      }
                    }
                } ~
                (get & authorizeAsync(check(cred, s"$instanceId/attachment", Read))) {
                  parameter('rev.as[Long].?) { revOpt =>
                    traceName("getInstanceAttachment") {
                      val result = revOpt match {
                        case Some(rev) => instances.fetchAttachment(instanceId, rev)
                        case None      => instances.fetchAttachment(instanceId)
                      }
                      onSuccess(result) {
                        case Some((info, source)) =>
                          val ct =
                            ContentType.parse(info.contentType).getOrElse(ContentTypes.`application/octet-stream`)
                          complete(HttpEntity(ct, info.size.value, source))
                        case None =>
                          complete(StatusCodes.NotFound)
                      }
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
    * @param instances     the instances operation bundle
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param base          the service public uri + prefix
    * @return a new ''InstanceRoutes'' instance
    */
  final def apply(instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
                  client: SparqlClient[Future],
                  querySettings: QuerySettings,
                  base: Uri)(implicit
                             ec: ExecutionContext,
                             mt: ActorMaterializer,
                             baseClient: UntypedHttpClient[Future],
                             filteringSettings: FilteringSettings,
                             iamUri: IamUri): InstanceRoutes = {
    implicit val qs: QuerySettings = querySettings
    val instanceQueries            = FilterQueries[Future, InstanceId](SparqlQuery[Future](client), querySettings)
    implicit val cl                = HttpClient.withAkkaUnmarshaller[AccessControlList]
    new InstanceRoutes(instances, instanceQueries, base)
  }
}

class InstanceCustomEncoders(base: Uri)(implicit le: Encoder[Link])
    extends RoutesEncoder[InstanceId, InstanceRef](base) {

  implicit val qualifierSchema: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)

  implicit val instanceEncoder: Encoder[Instance] = Encoder.encodeJson.contramap { instance =>
    val instanceRef = InstanceRef(instance.id, instance.rev, instance.attachment)
    val meta = instanceRefEncoder
      .apply(instanceRef)
      .deepMerge(
        Json.obj(
          "deprecated" -> Json.fromBoolean(instance.deprecated)
        ))
    instance.value.deepMerge(meta)
  }

  implicit val instanceRefEncoder: Encoder[InstanceRef] = Encoder.encodeJson.contramap { ref =>
    refEncoder.apply(ref) deepMerge ref.attachment.map(at => at.asJson).getOrElse(Json.obj())
  }

  implicit val instanceIdWithLinksEncoder: Encoder[InstanceId] = Encoder.encodeJson.contramap { instanceId =>
    idWithLinksEncoder.apply(instanceId) deepMerge
      Json.obj(
        "links" -> Json.arr(
          le(Link(rel = "self", href = instanceId.qualifyAsString)),
          le(Link(rel = "schema", href = instanceId.schemaId.qualifyAsString))
        ))
  }
}
