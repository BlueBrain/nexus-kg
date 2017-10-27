package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.CannotUnpublishSchema
import ch.epfl.bluebrain.nexus.kg.core.schemas._
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId, ShapeRef}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, FilteringSettings, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.{Publish, SchemaConfig}
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for schema specific functionality.
  *
  * @param schemas           the schemas operation bundle
  * @param schemaQueries     query builder for schemas
  * @param base              the service public uri + prefix
  */
class SchemaRoutes(schemas: Schemas[Future], schemaQueries: FilterQueries[Future, SchemaId], base: Uri)(
    implicit querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext)
    extends DefaultRouteHandling {

  private val schemaEncoders = new SchemaCustomEncoders(base)
  private val shapeEncoders  = new ShapeCustomEncoders(base)

  import schemaEncoders._
  import shapeEncoders.shapeEncoder

  private implicit val sQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)

  private val exceptionHandler = ExceptionHandling.exceptionHandler

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt) =>
      parameter('published.as[Boolean].?) { publishedOpt =>
        val filter = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
        traceName("searchSchemas") {
          pathEndOrSingleSlash {
            schemaQueries.list(filter, pagination, termOpt).buildResponse(base, pagination)
          } ~
            (extractAnyResourceId(3) & pathEndOrSingleSlash) { id =>
              schemaQueries.list(filter, pagination, termOpt).buildResponse(base, pagination)

              resourceId(id, of[OrgId]) { orgId =>
                schemaQueries.list(orgId, filter, pagination, termOpt).buildResponse(base, pagination)
              } ~
                resourceId(id, of[DomainId]) { domainId =>
                  schemaQueries.list(domainId, filter, pagination, termOpt).buildResponse(base, pagination)
                } ~
                resourceId(id, of[SchemaName]) { schemaName =>
                  schemaQueries.list(schemaName, filter, pagination, termOpt).buildResponse(base, pagination)
                }
            }
        }
      }
    }

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractResourceId(4, of[SchemaId]) { schemaId =>
      (pathEndOrSingleSlash & get & authorizeResource(schemaId, Read)) {
        traceName("getSchema") {
          onSuccess(schemas.fetch(schemaId)) {
            case Some(schema) => complete(schema)
            case None         => complete(StatusCodes.NotFound)
          }
        }
      } ~
        pathPrefix("shapes" / Segment) { fragment =>
          val shapeId = ShapeId(schemaId, fragment)
          (pathEndOrSingleSlash & get & authorizeResource(shapeId, Read)) {
            traceName("getSchemaShape") {
              onSuccess(schemas.fetchShape(schemaId, fragment)) {
                case Some(shapes) => complete(StatusCodes.OK -> shapes)
                case None         => complete(StatusCodes.NotFound)
              }
            }
          }
        }
    }

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractResourceId(4, of[SchemaId]) { schemaId =>
      pathEndOrSingleSlash {
        (put & entity(as[Json]) & authorizeResource(schemaId, Write)) { json =>
          parameter('rev.as[Long].?) {
            case Some(rev) =>
              traceName("updateSchema") {
                onSuccess(schemas.update(schemaId, rev, json)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            case None =>
              traceName("createSchema") {
                onSuccess(schemas.create(schemaId, json)) { ref =>
                  complete(StatusCodes.Created -> ref)
                }
              }
          }
        } ~
          (delete & authorizeResource(schemaId, Write)) {
            parameter('rev.as[Long]) { rev =>
              traceName("deprecateSchema") {
                onSuccess(schemas.deprecate(schemaId, rev)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          }
      } ~
        (path("config") & authorizeResource(s"$schemaId/config", Publish)) {
          (pathEndOrSingleSlash & patch & entity(as[SchemaConfig])) { cfg =>
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

  def routes: Route = combinedRoutesFor("schemas")

  private def publishedExpr(published: Option[Boolean]): Option[Expr] =
    published.map { value =>
      val pub = "published".qualify
      ComparisonExpr(Op.Eq, UriPath(pub), LiteralTerm(value.toString))
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
  final def apply(schemas: Schemas[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit
      ec: ExecutionContext,
      iamClient: IamClient[Future],
      filteringSettings: FilteringSettings): SchemaRoutes = {

    implicit val qs: QuerySettings = querySettings
    val schemaQueries              = FilterQueries[Future, SchemaId](SparqlQuery[Future](client), querySettings)
    new SchemaRoutes(schemas, schemaQueries, base)
  }

  /**
    * Local schema config definition that models a resource for schema state change operations.
    *
    * @param published whether the schema should be published or un-published
    */
  final case class SchemaConfig(published: Boolean)

  private[routes] val Publish = Permission("publish")

}

private class ShapeCustomEncoders(base: Uri) extends RoutesEncoder[ShapeId, ShapeRef](base) {

  implicit def shapeEncoder: Encoder[Shape] = Encoder.encodeJson.contramap { shape =>
    val meta = refEncoder
      .apply(ShapeRef(shape.id, shape.rev))
      .deepMerge(
        Json.obj(
          "deprecated" -> Json.fromBoolean(shape.deprecated),
          "published"  -> Json.fromBoolean(shape.published)
        ))
    shape.value.deepMerge(meta)
  }
}

class SchemaCustomEncoders(base: Uri) extends RoutesEncoder[SchemaId, SchemaRef](base) {

  implicit def schemaEncoder: Encoder[Schema] = Encoder.encodeJson.contramap { schema =>
    val meta = refEncoder
      .apply(SchemaRef(schema.id, schema.rev))
      .deepMerge(
        Json.obj(
          "deprecated" -> Json.fromBoolean(schema.deprecated),
          "published"  -> Json.fromBoolean(schema.published)
        ))
    schema.value.deepMerge(meta)
  }
}
