package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{PathMatchers, Route}
import akka.stream.Materializer
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{UntypedHttpClient, withAkkaUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.http.{ContextUri, HttpClient}
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResults, SortList}
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.CannotUnpublishSchema
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.ShapeId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, Schemas}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.query.schemas.SchemasElasticQueries
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.{Publish, SchemaConfig}
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.IdToEntityRetrieval._
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.{SchemaCustomEncoders, ShapeCustomEncoders}
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import kamon.akka.http.KamonTraceDirectives._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for schema specific functionality.
  *
  * @param schemas               the schemas operation bundle
  * @param schemaQueries         query builder for schemas
  * @param schemasElasticQueries Elastic search client for schemas
  * @param base                  the service public uri + prefix
  * @param prefixes              the service context URIs
  */
class SchemaRoutes(schemas: Schemas[Future],
                   schemaQueries: FilterQueries[Future, SchemaId],
                   schemasElasticQueries: SchemasElasticQueries[Future],
                   base: Uri)(implicit contexts: Contexts[Future],
                              querySettings: QuerySettings,
                              filteringSettings: FilteringSettings,
                              iamClient: IamClient[Future],
                              ec: ExecutionContext,
                              clock: Clock,
                              orderedKeys: OrderedKeys,
                              prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts) {

  private implicit val coreContext: ContextUri              = prefixes.CoreContext
  private implicit val schemaEncoders: SchemaCustomEncoders = new SchemaCustomEncoders(base, prefixes)
  private val shapeEncoders                                 = new ShapeCustomEncoders(base, prefixes)

  import schemaEncoders._
  import shapeEncoders.shapeEncoder

  private val exceptionHandler = ExceptionHandling.exceptionHandler(prefixes.ErrorContext)

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paramsToQuery) { (pagination, query) =>
      implicit val schemaIdExtractor = schemaIdToEntityRetrieval(schemas)

      operationName("searchSchemas") {
        (pathEndOrSingleSlash & getAcls("*" / "*")) { implicit acls =>
          (query.filter, query.q, query.sort) match {
            case (Filter.Empty, None, SortList.Empty) =>
              schemasElasticQueries
                .list(pagination, query.deprecated, query.published, acls)
                .buildResponse(query.fields, base, prefixes, pagination)
            case _ =>
              schemaQueries.list(query, pagination).buildResponse(query.fields, base, prefixes, pagination)
          }
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            getAcls(orgId.show / "*").apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  schemasElasticQueries
                    .list(pagination, orgId, query.deprecated, query.published, acls)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  schemaQueries
                    .list(orgId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractDomainId & pathEndOrSingleSlash) { domainId =>
            getAcls(Path(domainId.show)).apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  schemasElasticQueries
                    .list(pagination, domainId, query.deprecated, query.published, acls)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  schemaQueries
                    .list(domainId, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
              }
            }
          } ~
          (extractSchemaName & pathEndOrSingleSlash) { schemaName =>
            getAcls(Path(schemaName.domainId.show)).apply { implicit acls =>
              (query.filter, query.q, query.sort) match {
                case (Filter.Empty, None, SortList.Empty) =>
                  schemasElasticQueries
                    .list(pagination, schemaName, query.deprecated, query.published, acls)
                    .buildResponse(query.fields, base, prefixes, pagination)
                case _ =>
                  schemaQueries
                    .list(schemaName, query, pagination)
                    .buildResponse(query.fields, base, prefixes, pagination)
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
        pathPrefix("shapes" / PathMatchers.Segment) { fragment =>
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
}

object SchemaRoutes {

  /**
    * Constructs a new ''SchemaRoutes'' instance that defines the http routes specific to schemas.
    *
    * @param schemas         the schemas operation bundle
    * @param client          the sparql client
    * @param querySettings   query parameters form settings
    * @param elasticClient   Elastic Search client
    * @param elasticSettings Elastic Search settings
    * @param base            the service public uri + prefix
    * @param prefixes        the service context URIs
    * @return a new ''SchemaRoutes'' instance
    */
  final def apply(schemas: Schemas[Future],
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
                             prefixes: PrefixUris): SchemaRoutes = {

    implicit val qs: QuerySettings = querySettings
    val schemaQueries              = FilterQueries[Future, SchemaId](SparqlQuery[Future](client))

    implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](elasticSettings.base)
    implicit val D: Decoder[QueryResults[SchemaId]]                   = ElasticDecoder[SchemaId]
    implicit val rsSearch: HttpClient[Future, QueryResults[SchemaId]] = withAkkaUnmarshaller[QueryResults[SchemaId]]
    val schemasElasticQueries                                         = SchemasElasticQueries(elasticClient, elasticSettings)

    new SchemaRoutes(schemas, schemaQueries, schemasElasticQueries, base)
  }

  /**
    * Local schema config definition that models a resource for schema state change operations.
    *
    * @param published whether the schema should be published or un-published
    */
  final case class SchemaConfig(published: Boolean)

  private[routes] val Publish = Permission("publish")

}
