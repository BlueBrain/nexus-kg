package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
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
  * @param querySettings     query parameters from settings
  * @param filteringSettings filtering parameters from settings
  */
class InstanceRoutes(
  instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
  instanceQueries: FilterQueries[Future, InstanceId],
  base: Uri)(implicit querySettings: QuerySettings, filteringSettings: FilteringSettings) {

  private val encoders = new InstanceCustomEncoders(base)
  import encoders._

  def routes: Route = handleExceptions(ExceptionHandling.exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix("data") {
        (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
          traceName("searchInstances") {
            val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
            instanceQueries.list(filter, pagination, termOpt).buildResponse(base, pagination)
          }
        } ~
        pathPrefix(Segment) { orgIdString =>
          val orgId = OrgId(orgIdString)
          (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
            traceName("searchInstancesOfOrg") {
              val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
              instanceQueries.list(orgId, filter, pagination, termOpt).buildResponse(base, pagination)
            }

          } ~
          pathPrefix(Segment) { domain =>
            val domainId = DomainId(orgId, domain)
            (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                traceName("searchInstancesOfDomain") {
                  val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
                  instanceQueries.list(domainId, filter, pagination, termOpt).buildResponse(base, pagination)
                }
            } ~
            pathPrefix(Segment) { name =>
              (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                traceName("searchInstancesOfSchemaName") {
                  val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
                  instanceQueries.list(domainId, name, filter, pagination, termOpt).buildResponse(base, pagination)
                }
              } ~
              versioned.apply { version =>
                val schemaId = SchemaId(DomainId(orgId, domain), name, version)
                (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                  traceName("searchInstancesOfSchema") {
                    val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
                    instanceQueries.list(schemaId, filter, pagination, termOpt).buildResponse(base, pagination)
                  }
                } ~
                (pathEndOrSingleSlash & post) {
                  entity(as[Json]) { json =>
                    traceName("createInstance") {
                      onSuccess(instances.create(schemaId, json)) { ref =>
                        complete(StatusCodes.Created -> ref)
                      }
                    }
                  }
                } ~
                pathPrefix(Segment) { id =>
                  val instanceId = InstanceId(schemaId, id)
                  (pathPrefix("outgoing") & pathEndOrSingleSlash & get) {
                    searchQueryParams.apply { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                      val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
                      instanceQueries.outgoing(instanceId, filter, pagination, termOpt).buildResponse(base, pagination)
                    }
                  } ~
                  (pathPrefix("incoming") & pathEndOrSingleSlash & get) {
                    searchQueryParams.apply { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                      val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase)
                      instanceQueries.incoming(instanceId, filter, pagination, termOpt).buildResponse(base, pagination)
                    }
                  } ~
                  pathEndOrSingleSlash {
                    (put & entity(as[Json]) & parameter('rev.as[Long])) { (json, rev) =>
                      traceName("updateInstance") {
                        onSuccess(instances.update(instanceId, rev, json)) { ref =>
                          complete(StatusCodes.OK -> ref)
                        }
                      }
                    } ~
                    get {
                      parameter('rev.as[Long].?) {
                        case Some(rev) =>
                          traceName("getInstanceRevision") {
                            onSuccess(instances.fetch(instanceId, rev)) {
                              case Some(instance) => complete(StatusCodes.OK -> instance)
                              case None           => complete(StatusCodes.NotFound)
                            }
                          }
                        case None      =>
                          traceName("getInstance") {
                            onSuccess(instances.fetch(instanceId)) {
                              case Some(instance) => complete(StatusCodes.OK -> instance)
                              case None           => complete(StatusCodes.NotFound)
                            }
                          }
                      }
                    } ~
                    (delete & parameter('rev.as[Long])) { rev =>
                      traceName("deprecateInstance") {
                        onSuccess(instances.deprecate(instanceId, rev)) { ref =>
                          complete(StatusCodes.OK -> ref)
                        }
                      }
                    }
                  } ~
                  path("attachment") {
                    (put & parameter('rev.as[Long])) { rev =>
                      fileUpload("file") {
                        case (metadata, byteSource) =>
                          traceName("createInstanceAttachment") {
                            onSuccess(instances.createAttachment(instanceId, rev, metadata.fileName, metadata.contentType.value, byteSource)) { info =>
                              complete(StatusCodes.Created -> info)
                            }
                          }
                      }
                    } ~
                    (delete & parameter('rev.as[Long])) { rev =>
                      traceName("removeInstanceAttachment") {
                        onSuccess(instances.removeAttachment(instanceId, rev)) { ref =>
                          complete(StatusCodes.OK -> ref)
                        }
                      }
                    } ~
                    get {
                      parameter('rev.as[Long].?) { maybeRev =>
                        traceName("getInstanceAttachment") {
                          val result = maybeRev match {
                            case Some(rev) => instances.fetchAttachment(instanceId, rev)
                            case None      => instances.fetchAttachment(instanceId)
                          }
                          onSuccess(result) {
                            case Some((info, source)) =>
                              val ct = ContentType.parse(info.contentType).getOrElse(ContentTypes.`application/octet-stream`)
                              complete(HttpEntity(ct, info.size.value, source))
                            case None                 =>
                              complete(StatusCodes.NotFound)
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
      }
    }
  }
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
    filteringSettings: FilteringSettings): InstanceRoutes = {
    implicit val qs: QuerySettings = querySettings
    val instanceQueries = FilterQueries[Future, InstanceId](SparqlQuery[Future](client), querySettings)
    new InstanceRoutes(instances, instanceQueries, base)
  }
}

private class InstanceCustomEncoders(base: Uri)(implicit le: Encoder[Link]) extends RoutesEncoder[InstanceId, InstanceRef](base) {

  implicit val qualifierSchema: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)

  implicit val instanceEncoder: Encoder[Instance] = Encoder.encodeJson.contramap { instance =>
    val instanceRef = InstanceRef(instance.id, instance.rev, instance.attachment)
    val meta = instanceRefEncoder.apply(instanceRef).deepMerge(Json.obj(
      "deprecated" -> Json.fromBoolean(instance.deprecated)
    ))
    instance.value.deepMerge(meta)
  }

  implicit val instanceRefEncoder: Encoder[InstanceRef] = Encoder.encodeJson.contramap { ref =>
    refEncoder.apply(ref) deepMerge ref.attachment.map(at => at.asJson).getOrElse(Json.obj())
  }

  implicit val instanceIdWithLinksEncoder: Encoder[InstanceId] = Encoder.encodeJson.contramap { instanceId =>
    idWithLinksEncoder.apply(instanceId) deepMerge
      Json.obj("links" -> Json.arr(
        le(Link(rel = "self", href = instanceId.qualifyAsString)),
        le(Link(rel = "schema", href = instanceId.schemaId.qualifyAsString))
      ))
  }

}