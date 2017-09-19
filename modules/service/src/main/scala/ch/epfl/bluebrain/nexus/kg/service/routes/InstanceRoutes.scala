package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaRejection}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Filter, FilteringSettings, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QueryResults, QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.query.{InstanceQueries, LinksQueryResults}
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives.traceName
import ch.epfl.bluebrain.nexus.kg.service.query.InstanceQueries._
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
  instanceQueries: InstanceQueries,
  base: Uri)(implicit querySettings: QuerySettings, filteringSettings: FilteringSettings) {

  private val encoders = new InstanceCustomEncoders(base)
  import encoders._

  private val exceptionHandler = ExceptionHandling.exceptionHandler

  def routes: Route = handleExceptions(exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix("data" / Segment) { orgIdString =>
        val orgId = OrgId(orgIdString)
        pathEndOrSingleSlash {
          (get & paginatedAndFiltered) { (pagination, filterOpt) =>
            traceName("listInstancesOfOrg") {
              parameter('deprecated.as[Boolean].?) { deprecated =>
                val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                buildResponse(instanceQueries.list(orgId, filter, pagination), pagination)
              }
            }
          }
        } ~
        pathPrefix(Segment) { domain =>
          val domainId = DomainId(orgId, domain)
          pathEndOrSingleSlash {
            (get & paginatedAndFiltered) { (pagination, filterOpt) =>
              parameter('deprecated.as[Boolean].?) { deprecated =>
                traceName("listInstancesOfDomain") {
                  val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                  buildResponse(instanceQueries.list(domainId, filter, pagination), pagination)
                }
              }
            }
          } ~
          pathPrefix(Segment) { name =>
            pathEndOrSingleSlash {
              (get & paginatedAndFiltered) { (pagination, filterOpt) =>
                parameter('deprecated.as[Boolean].?) { deprecated =>
                  traceName("listInstancesOfSchemaName") {
                    val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                    buildResponse(instanceQueries.list(domainId, name, filter, pagination), pagination)
                  }
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
                    (get & paginatedAndFiltered) { (pagination, filterOpt) =>
                      parameter('deprecated.as[Boolean].?) { deprecated =>
                        traceName("listInstancesOfSchema") {
                          val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                          buildResponse(instanceQueries.list(schemaId, filter, pagination), pagination)
                        }
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
                    (pathPrefix("outgoing") & pathEndOrSingleSlash & get) {
                      parameter('deprecated.as[Boolean].?) { deprecated =>
                        paginatedAndFiltered.apply { (pagination, filterOpt) =>
                          val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                          buildResponse(instanceQueries.outgoing(instanceId, filter, pagination), pagination)
                        }
                      }
                    } ~
                    (pathPrefix("incoming") & pathEndOrSingleSlash & get) {
                      parameter('deprecated.as[Boolean].?) { deprecated =>
                        paginatedAndFiltered.apply { (pagination, filterOpt) =>
                          val filter = Filter(deprecatedAndRev(deprecated)) and filterOpt.map(_.expr)
                          buildResponse(instanceQueries.incoming(instanceId, filter, pagination), pagination)
                        }
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

  private def deprecatedOrNoop(deprecated: Option[Boolean]): Expr = {
    deprecated.map { value =>
      val depr = "deprecated".qualifyWith(querySettings.nexusVocBase)
      ComparisonExpr(Op.Eq, UriTerm(depr), LiteralTerm(value.toString))
    }.getOrElse(NoopExpr)
  }

  private def deprecatedAndRev(deprecated: Option[Boolean]): Expr = {
    LogicalExpr(Op.And, List(deprecatedOrNoop(deprecated), revExpr))
  }

  private def revExpr: Expr = {
    val rev = "rev".qualifyWith(querySettings.nexusVocBase)
    ComparisonExpr(Op.Gt, UriTerm(rev), LiteralTerm("0"))
  }


  private def buildResponse[A](qr: Future[QueryResults[A]], pagination: Pagination)(implicit
    R: Encoder[UnscoredQueryResult[A]],
    S: Encoder[ScoredQueryResult[A]]): Route =
    extract(_.request.uri) { uri =>
      onSuccess(qr) { result =>
        val lqu = base.copy(path = uri.path, fragment = uri.fragment, rawQueryString = uri.rawQueryString)
        val lqrs = LinksQueryResults(result, pagination, lqu)
        complete(StatusCodes.OK -> lqrs)
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
    val instanceQueries = new InstanceQueries(SparqlQuery[Future](client), querySettings, base)
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