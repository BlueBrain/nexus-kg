package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.{CannotUnpublishSchema, IllegalVersionFormat}
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId, ShapeRef}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, FilteringSettings, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for schema specific functionality.
  *
  * @param schemas       the schemas operation bundle
  * @param schemaQueries query builder for schemas
  * @param base          the service public uri + prefix
  * @param querySettings     query parameters from settings
  * @param filteringSettings filtering parameters from settings
  */
class SchemaRoutes(schemas: Schemas[Future], schemaQueries: FilterQueries[Future, SchemaId], base: Uri)(implicit querySettings: QuerySettings, filteringSettings: FilteringSettings) {

  private val schemaEncoders = new SchemaCustomEncoders(base)
  private val shapeEncoders = new ShapeCustomEncoders(base)

  import schemaEncoders._
  import shapeEncoders.shapeEncoder

  private implicit val sQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)

  private val exceptionHandler = ExceptionHandling.exceptionHandler

  def routes: Route = handleExceptions(exceptionHandler) {
    handleRejections(RejectionHandling.rejectionHandler) {
      pathPrefix("schemas") {
        (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
          parameter('published.as[Boolean].?) { publishedOpt =>
            traceName("listSchemasOfOrg") {
              val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
              schemaQueries.list(filter, pagination, termOpt) buildResponse(base, pagination)
            }
          }
        } ~
        pathPrefix(Segment) { orgIdString =>
          val orgId = OrgId(orgIdString)
          (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
            parameter('published.as[Boolean].?) { publishedOpt =>
              traceName("listSchemasOfOrg") {
                val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
                schemaQueries.list(orgId, filter, pagination, termOpt) buildResponse(base, pagination)
              }
            }
          } ~
          pathPrefix(Segment) { domain =>
            val domainId = DomainId(orgId, domain)
            (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
              parameter('published.as[Boolean].?) { publishedOpt =>
                traceName("listSchemasOfDomain") {
                  val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
                  schemaQueries.list(domainId, filter, pagination, termOpt) buildResponse(base, pagination)
                }
              }
            } ~
            pathPrefix(Segment) { name =>
              (pathEndOrSingleSlash & get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
                parameter('published.as[Boolean].?) { publishedOpt =>
                  traceName("listSchemasOfSchemaName") {
                    val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
                    schemaQueries.list(domainId, name, filter, pagination, termOpt) buildResponse(base, pagination)
                  }
                }
              } ~
              pathPrefix(Segment) { versionString =>
                Version(versionString) match {
                  case None          =>
                    exceptionHandler(CommandRejected(IllegalVersionFormat))
                  case Some(version) =>
                    val schemaId = SchemaId(domainId, name, version)
                    pathEnd {
                      (put & entity(as[Json])) { json =>
                        parameter('rev.as[Long].?) {
                          case Some(rev) =>
                            traceName("updateSchema") {
                              onSuccess(schemas.update(schemaId, rev, json)) { ref =>
                                complete(StatusCodes.OK -> ref)
                              }
                            }
                          case None      =>
                            traceName("createSchema") {
                              onSuccess(schemas.create(schemaId, json)) { ref =>
                                complete(StatusCodes.Created -> ref)
                              }
                            }
                        }
                      } ~
                      get {
                        traceName("getSchema") {
                          onSuccess(schemas.fetch(schemaId)) {
                            case Some(schema) => complete(StatusCodes.OK -> schema)
                            case None         => complete(StatusCodes.NotFound)
                          }
                        }
                      } ~
                      delete {
                        parameter('rev.as[Long]) { rev =>
                          traceName("deprecateSchema") {
                            onSuccess(schemas.deprecate(schemaId, rev)) { ref =>
                              complete(StatusCodes.OK -> ref)
                            }
                          }
                        }
                      }
                    } ~
                    pathPrefix("shapes" / Segment) { fragment =>
                      get {
                        traceName("getSchemaShape") {
                          onSuccess(schemas.fetchShape(schemaId, fragment)) {
                            case Some(shapes) => complete(StatusCodes.OK -> shapes)
                            case None         => complete(StatusCodes.NotFound)
                          }
                        }
                      }
                    } ~
                    path("config") {
                      (patch & entity(as[SchemaConfig])) { cfg =>
                        parameter('rev.as[Long]) { rev =>
                          if (cfg.published) {
                            traceName("publishSchema") {
                              onSuccess(schemas.publish(schemaId, rev)) { ref =>
                                complete(StatusCodes.OK -> ref)
                              }
                            }
                          } else exceptionHandler(CommandRejected(CannotUnpublishSchema))
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

  private def publishedExpr(published: Option[Boolean]): Option[Expr] =
    published.map { value =>
      val pub = "published".qualify
      ComparisonExpr(Op.Eq, UriTerm(pub), LiteralTerm(value.toString))
    }
}

object SchemaRoutes {

  /**
    * Constructs a new ''SchemaRoutes'' instance that defines the http routes specific to schemas.
    *
    * @param schemas       the schemas operation bundle
    * @param client        the sparql client
    * @param querySettings query parameters form settings
    * @param base          the service public uri + prefix
    * @return a new ''SchemaRoutes'' instance
    */
  final def apply(schemas: Schemas[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(implicit
    ec: ExecutionContext, filteringSettings: FilteringSettings): SchemaRoutes = {
    implicit val qs: QuerySettings = querySettings
    val schemaQueries = FilterQueries[Future, SchemaId](SparqlQuery[Future](client), querySettings)
    new SchemaRoutes(schemas, schemaQueries, base)
  }

  /**
    * Local schema config definition that models a resource for schema state change operations.
    *
    * @param published whether the schema should be published or un-published
    */
  final case class SchemaConfig(published: Boolean)

}

private class ShapeCustomEncoders(base: Uri) extends RoutesEncoder[ShapeId, ShapeRef](base) {

  implicit def shapeEncoder: Encoder[Shape] = Encoder.encodeJson.contramap { shape =>
    val meta = refEncoder.apply(ShapeRef(shape.id, shape.rev)).deepMerge(Json.obj(
      "deprecated" -> Json.fromBoolean(shape.deprecated),
      "published" -> Json.fromBoolean(shape.published)
    ))
    shape.value.deepMerge(meta)
  }
}

private class SchemaCustomEncoders(base: Uri) extends RoutesEncoder[SchemaId, SchemaRef](base) {

  implicit def schemaEncoder: Encoder[Schema] = Encoder.encodeJson.contramap { schema =>
    val meta = refEncoder.apply(SchemaRef(schema.id, schema.rev)).deepMerge(Json.obj(
      "deprecated" -> Json.fromBoolean(schema.deprecated),
      "published" -> Json.fromBoolean(schema.published)
    ))
    schema.value.deepMerge(meta)
  }
}

