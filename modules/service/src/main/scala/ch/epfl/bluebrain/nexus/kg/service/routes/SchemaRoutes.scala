package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.CannotUnpublishSchema
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId, ShapeRef}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef, Schemas}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{FilteringSettings, Op}
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
  * @param prefixes          the service context URIs
  */
class SchemaRoutes(schemas: Schemas[Future], schemaQueries: FilterQueries[Future, SchemaId], base: Uri)(
    implicit
    contexts: Contexts[Future],
    querySettings: QuerySettings,
    filteringSettings: FilteringSettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext,
    clock: Clock,
    orderedKeys: OrderedKeys,
    prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts) {
  private implicit val schemaIdExtractor = (entity: Schema) => entity.id
  private implicit val shapeIdExtractor  = (entity: Shape) => entity.id
  private implicit val sQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)

  private implicit val coreContext: ContextUri              = prefixes.CoreContext
  private implicit val schemaEncoders: SchemaCustomEncoders = new SchemaCustomEncoders(base, prefixes)
  private val shapeEncoders                                 = new ShapeCustomEncoders(base, prefixes)

  import schemaEncoders._
  import shapeEncoders.shapeEncoder

  private val exceptionHandler = ExceptionHandling.exceptionHandler(prefixes.ErrorContext)

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & searchQueryParams) { (pagination, filterOpt, termOpt, deprecatedOpt, fields, sort) =>
      parameter('published.as[Boolean].?) { publishedOpt =>
        val filter     = filterFrom(deprecatedOpt, filterOpt, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
        implicit val _ = (id: SchemaId) => schemas.fetch(id)
        operationName("searchSchemas") {
          (pathEndOrSingleSlash & authenticateCaller) { implicit caller =>
            schemaQueries.list(filter, pagination, termOpt, sort).buildResponse(fields, base, prefixes, pagination)
          } ~
            (extractOrgId & pathEndOrSingleSlash) { orgId =>
              authenticateCaller.apply { implicit caller =>
                schemaQueries
                  .list(orgId, filter, pagination, termOpt, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            } ~
            (extractDomainId & pathEndOrSingleSlash) { domainId =>
              authenticateCaller.apply { implicit caller =>
                schemaQueries
                  .list(domainId, filter, pagination, termOpt, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            } ~
            (extractSchemaName & pathEndOrSingleSlash) { schemaName =>
              authenticateCaller.apply { implicit caller =>
                schemaQueries
                  .list(schemaName, filter, pagination, termOpt, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            }
        }
      }
    }

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractSchemaId { schemaId =>
      (pathEndOrSingleSlash & get & authorizeResource(schemaId, Read) & format) { format =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            operationName("getSchemaRevision") {
              onSuccess(schemas.fetch(schemaId, rev)) {
                case Some(schema) => formatOutput(schema, format)
                case None         => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            operationName("getSchema") {
              onSuccess(schemas.fetch(schemaId)) {
                case Some(schema) => formatOutput(schema, format)
                case None         => complete(StatusCodes.NotFound)
              }
            }
        }
      } ~
        pathPrefix("shapes" / Segment) { fragment =>
          val shapeId = ShapeId(schemaId, fragment)
          (pathEndOrSingleSlash & get & authorizeResource(shapeId, Read) & format) { format =>
            parameter('rev.as[Long].?) {
              case Some(rev) =>
                operationName("getSchemaShapeRevision") {
                  onSuccess(schemas.fetchShape(schemaId, fragment, rev)) {
                    case Some(shape) => formatOutput(shape, format)
                    case None        => complete(StatusCodes.NotFound)
                  }
                }
              case None =>
                operationName("getSchemaShape") {
                  onSuccess(schemas.fetchShape(schemaId, fragment)) {
                    case Some(shape) => formatOutput(shape, format)
                    case None        => complete(StatusCodes.NotFound)
                  }
                }
            }
          }
        }
    }

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractSchemaId { schemaId =>
      pathEndOrSingleSlash {
        (put & entity(as[Json])) { json =>
          (authenticateCaller & authorizeResource(schemaId, Write)) { implicit caller =>
            parameter('rev.as[Long].?) {
              case Some(rev) =>
                operationName("updateSchema") {
                  onSuccess(schemas.update(schemaId, rev, json)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              case None =>
                operationName("createSchema") {
                  onSuccess(schemas.create(schemaId, json)) { ref =>
                    complete(StatusCodes.Created -> ref)
                  }
                }
            }
          }
        } ~
          (delete & parameter('rev.as[Long])) { rev =>
            (authenticateCaller & authorizeResource(schemaId, Write)) { implicit caller =>
              operationName("deprecateSchema") {
                onSuccess(schemas.deprecate(schemaId, rev)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          }
      } ~
        path("config") {
          (pathEndOrSingleSlash & patch & entity(as[SchemaConfig]) & parameter('rev.as[Long])) { (cfg, rev) =>
            (authenticateCaller & authorizeResource(schemaId, Publish)) { implicit caller =>
              if (cfg.published) {
                operationName("publishSchema") {
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
    * @param prefixes      the service context URIs
    * @return a new ''SchemaRoutes'' instance
    */
  final def apply(schemas: Schemas[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit
      contexts: Contexts[Future],
      ec: ExecutionContext,
      iamClient: IamClient[Future],
      filteringSettings: FilteringSettings,
      clock: Clock,
      orderedKeys: OrderedKeys,
      prefixes: PrefixUris): SchemaRoutes = {

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

private class ShapeCustomEncoders(base: Uri, prefixes: PrefixUris)(implicit E: Shape => ShapeId)
    extends RoutesEncoder[ShapeId, ShapeRef, Shape](base, prefixes) {

  implicit val shapeRefEncoder: Encoder[ShapeRef] = refEncoder

  implicit def shapeEncoder: Encoder[Shape] = Encoder.encodeJson.contramap { shape =>
    val meta = refEncoder
      .apply(ShapeRef(shape.id, shape.rev))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(shape.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(shape.published)
        ))
    shape.value.deepMerge(meta)
  }
}

class SchemaCustomEncoders(base: Uri, prefixes: PrefixUris)(implicit E: Schema => SchemaId)
    extends RoutesEncoder[SchemaId, SchemaRef, Schema](base, prefixes) {

  implicit val schemaRefEncoder: Encoder[SchemaRef] = refEncoder

  implicit def schemaEncoder: Encoder[Schema] = Encoder.encodeJson.contramap { schema =>
    val meta = refEncoder
      .apply(SchemaRef(schema.id, schema.rev))
      .deepMerge(idWithLinksEncoder(schema.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(schema.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(schema.published)
        ))
    schema.value.deepMerge(meta)
  }
}
