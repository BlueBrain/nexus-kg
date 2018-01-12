package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection.CannotUnpublishContext
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.Publish
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json, Printer}
import kamon.akka.http.KamonTraceDirectives.traceName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for context specific functionality
  *
  * @param contexts       the context operation bundle
  * @param contextQueries query builder for contexts
  * @param base           the service public uri + prefix
  * @param prefixes       the service context URIs
  * @param iamClient      IAM client
  * @param ec             execution context
  * @param clock          the clock used to issue instants
  */
class ContextRoutes(contexts: Contexts[Future], contextQueries: FilterQueries[Future, ContextId], base: Uri)(
    implicit
    querySettings: QuerySettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext,
    clock: Clock,
    orderedKeys: OrderedKeys,
    prefixes: PrefixUris)
    extends DefaultRouteHandling(contexts)
    with QueryDirectives {

  private implicit val _ = (entity: Context) => entity.id
  private implicit val qualifier: ConfiguredQualifier[String] = Qualifier.configured[String](querySettings.nexusVocBase)
  private implicit val contextEncoders: ContextCustomEncoders = new ContextCustomEncoders(base, prefixes)
  import contextEncoders._

  private val exceptionHandler = ExceptionHandling.exceptionHandler(prefixes.ErrorContext)

  override protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    extractContextId { contextId =>
      pathEndOrSingleSlash {
        (put & entity(as[Json])) { json =>
          (authenticateCaller & authorizeResource(contextId, Write)) { implicit caller =>
            parameter('rev.as[Long].?) {
              case Some(rev) =>
                traceName("updateContext") {
                  onSuccess(contexts.update(contextId, rev, json)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              case None =>
                traceName("createContext") {
                  onSuccess(contexts.create(contextId, json)) { ref =>
                    complete(StatusCodes.Created -> ref)

                  }
                }
            }
          }

        } ~
          (delete & parameter('rev.as[Long])) { rev =>
            (authenticateCaller & authorizeResource(contextId, Write)) { implicit caller =>
              traceName("deprecateContext") {
                onSuccess(contexts.deprecate(contextId, rev)) { ref =>
                  complete(StatusCodes.OK -> ref)
                }
              }
            }
          }
      } ~
        path("config") {
          (pathEndOrSingleSlash & patch & entity(as[ContextConfig]) & parameter('rev.as[Long])) { (cfg, rev) =>
            (authenticateCaller & authorizeResource(contextId, Publish)) { implicit caller =>
              if (cfg.published) {
                traceName("publishSchema") {
                  onSuccess(contexts.publish(contextId, rev)) { ref =>
                    complete(StatusCodes.OK -> ref)
                  }
                }
              } else exceptionHandler(CommandRejected(CannotUnpublishContext))
            }
          }
        }
    }

  override protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route = {
    import contextEncoders.marshallerHttp
    extractContextId { contextId =>
      (pathEndOrSingleSlash & get & authorizeResource(contextId, Read) & format) { format =>
        parameter('rev.as[Long].?) {
          case Some(rev) =>
            traceName("getContextRevision") {
              onSuccess(contexts.fetch(contextId, rev)) {
                case Some(context) => formatOutput(context, format)
                case None          => complete(StatusCodes.NotFound)
              }
            }
          case None =>
            traceName("getContext") {
              onSuccess(contexts.fetch(contextId)) {
                case Some(context) => formatOutput(context, format)
                case None          => complete(StatusCodes.NotFound)
              }
            }
        }
      }
    }
  }

  override protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paginated & deprecated & published & fields & sort) {
      (pagination, deprecatedOpt, publishedOpt, fields, sort) =>
        traceName("searchContexts") {
          val filter     = filterFrom(deprecatedOpt, None, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
          implicit val _ = (id: ContextId) => contexts.fetch(id)
          (pathEndOrSingleSlash & authenticateCaller) { implicit caller =>
            contextQueries.list(filter, pagination, None, sort).buildResponse(fields, base, prefixes, pagination)
          } ~
            (extractOrgId & pathEndOrSingleSlash) { orgId =>
              authenticateCaller.apply { implicit caller =>
                contextQueries
                  .list(orgId, filter, pagination, None, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            } ~
            (extractDomainId & pathEndOrSingleSlash) { domainId =>
              authenticateCaller.apply { implicit caller =>
                contextQueries
                  .list(domainId, filter, pagination, None, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            } ~
            (extractContextName & pathEndOrSingleSlash) { contextName =>
              authenticateCaller.apply { implicit caller =>
                contextQueries
                  .list(contextName, filter, pagination, None, sort)
                  .buildResponse(fields, base, prefixes, pagination)
              }
            }
        }
    }

  def routes: Route = combinedRoutesFor("contexts")

  private def publishedExpr(published: Option[Boolean]): Option[Expr] =
    published.map { value =>
      val pub = "published".qualify
      ComparisonExpr(Op.Eq, UriPath(pub), LiteralTerm(value.toString))
    }

}

object ContextRoutes {

  /**
    * Constructs a new ''ContextRoutes'' instance that defines the the http routes specific to contexts.
    *
    * @param contexts  the context operation bundle
    * @param base      the service public uri + prefix
    * @param prefixes  the service context URIs
    * @param iamClient IAM client
    * @param ec        execution context
    * @param clock     the clock used to issue instants
    * @return a new ''ContextRoutes'' instance
    */
  final def apply(contexts: Contexts[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit iamClient: IamClient[Future],
      ec: ExecutionContext,
      clock: Clock,
      orderedKeys: OrderedKeys,
      prefixes: PrefixUris): ContextRoutes = {

    implicit val qs: QuerySettings = querySettings
    val contextQueries             = FilterQueries[Future, ContextId](SparqlQuery[Future](client), querySettings)
    new ContextRoutes(contexts, contextQueries, base)
  }

  /**
    * Local context config definition that models a resource for context state change operations
    *
    * @param published whether the context should be published or un-published
    */
  final case class ContextConfig(published: Boolean)

}

class ContextCustomEncoders(base: Uri, prefixes: PrefixUris)(implicit E: Context => ContextId)
    extends RoutesEncoder[ContextId, ContextRef, Context](base, prefixes) {

  implicit val contextRefEncoder: Encoder[ContextRef] = refEncoder.mapJson(_.addCoreContext)

  implicit val contextEncoder: Encoder[Context] = Encoder.encodeJson.contramap { ctx =>
    val meta = refEncoder
      .apply(ContextRef(ctx.id, ctx.rev))
      .deepMerge(idWithLinksEncoder(ctx.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(ctx.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(ctx.published)
        )
      )
    if (ctx.id.qualify == prefixes.CoreContext.context)
      ctx.value.deepMerge(meta)
    else
      ctx.value.deepMerge(meta).addCoreContext
  }

  /**
    * Custom marshaller that doesn't inject the context into the JSON-LD payload.
    *
    * @return a [[Context]] marshaller
    */
  implicit def marshallerHttp(implicit
                              encoder: Encoder[Context],
                              printer: Printer = Printer.noSpaces.copy(dropNullKeys = true),
                              keys: OrderedKeys = OrderedKeys()): ToEntityMarshaller[Context] =
    jsonLdMarshaller.compose(encoder.apply)
}
