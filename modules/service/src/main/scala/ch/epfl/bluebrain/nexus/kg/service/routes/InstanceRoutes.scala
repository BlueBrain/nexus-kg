package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{parameter, _}
import akka.http.scaladsl.server.Route
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.instances.future._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaRejection}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.query.FilterQueries
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._
import io.circe.syntax._
import scala.concurrent.{ExecutionContext, Future}


/**
  * Http route definitions for instance specific functionality.
  *
  * @param instances     the instances operation bundle
  * @param querySettings query parameters form settings
  * @param queryBuilder  query builder for schemas
  * @param base          the service public uri + prefix
  */
class InstanceRoutes(
  instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
  querySettings: QuerySettings,
  queryBuilder: FilterQueries[InstanceId],
  base: Uri) {

  private val encoders = new InstanceCustomEncoders(base)

  import encoders._, queryBuilder._

  private val exceptionHandler = ExceptionHandling.exceptionHandler
  private val pagination = querySettings.pagination

  def routes: Route = handleExceptions(exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix("data" / Segment) { orgIdString =>
        val orgId = OrgId(orgIdString)
        pathEndOrSingleSlash {
          (get & parameter('from.as[Int] ? pagination.from) & parameter('size.as[Int] ? pagination.size) & parameter('deprecated.as[Boolean].?)) { (from, size, deprecated) =>
            traceName("listInstancesOfOrg") {
              queryBuilder.listingQuery(orgId, deprecated, None, Pagination(from, size)).response
            }
          }
        } ~
        pathPrefix(Segment) { domain =>
          val domainId = DomainId(orgId, domain)
          pathEndOrSingleSlash {
            (get & parameter('from.as[Int] ? pagination.from) & parameter('size.as[Int] ? pagination.size) & parameter('deprecated.as[Boolean].?)) { (from, size, deprecated) =>
              traceName("listInstancesOfDomain") {
                queryBuilder.listingQuery(domainId, deprecated, None, Pagination(from, size)).response
              }
            }
          } ~
          pathPrefix(Segment) { name =>
            pathEndOrSingleSlash {
              (get & parameter('from.as[Int] ? pagination.from) & parameter('size.as[Int] ? pagination.size) & parameter('deprecated.as[Boolean].?)) { (from, size, deprecated) =>
                traceName("listInstancesOfSchemaName") {
                  queryBuilder.listingQuery(domainId, name, deprecated, None, Pagination(from, size)).response
                }
              }
            } ~
            pathPrefix(Segment) { versionString =>
              Version(versionString) match {
                case None          =>
                  exceptionHandler(CommandRejected(SchemaRejection.IllegalVersionFormat))
                case Some(version) =>
                  val schemaId = SchemaId(DomainId(orgId, domain), name, version)

                  pathEndOrSingleSlash {
                    (get & parameter('from.as[Int] ? pagination.from) & parameter('size.as[Int] ? pagination.size) & parameter('deprecated.as[Boolean].?)) { (from, size, deprecated) =>
                      traceName("listInstancesOfSchema") {
                        queryBuilder.listingQuery(domainId, schemaId, deprecated, None, Pagination(from, size)).response
                      }
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
                    pathEnd {
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
                          case (metadata, byteSource) => {
                            traceName("createInstanceAttachment") {
                              onSuccess(instances.createAttachment(instanceId, rev, metadata.fileName, metadata.contentType.value, byteSource)) { info =>
                                complete(StatusCodes.Created -> info)
                              }
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
    ec: ExecutionContext): InstanceRoutes = {
    val filterQueries = new FilterQueries[InstanceId](SparqlQuery[Future](client), querySettings, base)
    new InstanceRoutes(instances, querySettings, filterQueries, base)
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