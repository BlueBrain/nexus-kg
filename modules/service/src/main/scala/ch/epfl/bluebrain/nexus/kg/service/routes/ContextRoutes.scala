package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import cats.instances.future._
import cats.instances.string._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.CannotUnpublishSchema
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Op}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.indexing.query.{QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.Publish
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchResponse._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives.traceName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for context specific functionality
  *
  * @param contexts       the context operation bundle
  * @param contextQueries query builder for contexts
  * @param base           the service public uri + prefix
  * @param iamClient      IAM client
  * @param ec             execution context
  * @param clock          the clock used to issue instants
  */
class ContextRoutes(contexts: Contexts[Future], contextQueries: FilterQueries[Future, ContextId], base: Uri)(
    implicit
    querySettings: QuerySettings,
    iamClient: IamClient[Future],
    ec: ExecutionContext,
    clock: Clock)
    extends DefaultRouteHandling
    with QueryDirectives {

  private implicit val _ = (entity: Context) => entity.id
  private implicit val sQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](querySettings.nexusVocBase)
  private val contextEncoders = new ContextCustomEncoders(base)
  import contextEncoders._

  private val exceptionHandler = ExceptionHandling.exceptionHandler

  override protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]) =
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
              } else exceptionHandler(CommandRejected(CannotUnpublishSchema))
            }
          }
        }
    }

  override protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]) = {
    extractContextId { contextId =>
      (pathEndOrSingleSlash & get & authorizeResource(contextId, Read)) {
        traceName("getContext") {
          onSuccess(contexts.fetch(contextId)) {
            case Some(context) => complete(context)
            case None          => complete(StatusCodes.NotFound)
          }
        }
      }

    }
  }

  override protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route =
    (get & paginated & deprecated & published & fields) { (pagination, deprecatedOpt, publishedOpt, fields) =>
      traceName("searchContexts") {
        val filter     = filterFrom(deprecatedOpt, None, querySettings.nexusVocBase) and publishedExpr(publishedOpt)
        implicit val _ = (id: ContextId) => contexts.fetch(id)
        (pathEndOrSingleSlash & authenticateCaller) { implicit caller =>
          contextQueries.list(filter, pagination, None).buildResponse(fields, base, pagination)
        } ~
          (extractOrgId & pathEndOrSingleSlash) { orgId =>
            authenticateCaller.apply { implicit caller =>
              contextQueries.list(orgId, filter, pagination, None).buildResponse(fields, base, pagination)
            }
          } ~
          (extractDomainId & pathEndOrSingleSlash) { domainId =>
            authenticateCaller.apply { implicit caller =>
              contextQueries.list(domainId, filter, pagination, None).buildResponse(fields, base, pagination)
            }
          } ~
          (extractContextName & pathEndOrSingleSlash) { contextName =>
            authenticateCaller.apply { implicit caller =>
              contextQueries.list(contextName, filter, pagination, None).buildResponse(fields, base, pagination)
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
    * Contstructs ne ''ContextRoutes'' instance that defines the the http routes specific to contexts.
    *
    * @param contexts  the context operation bundle
    * @param base      the service public uri + prefix
    * @param iamClient IAM client
    * @param ec        execution context
    * @param clock     the clock used to issue instants
    * @return a new ''ContextRoutes'' instance
    */
  final def apply(contexts: Contexts[Future], client: SparqlClient[Future], querySettings: QuerySettings, base: Uri)(
      implicit iamClient: IamClient[Future],
      ec: ExecutionContext,
      clock: Clock): ContextRoutes = {

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

class ContextCustomEncoders(base: Uri)(implicit E: Context => ContextId)
    extends RoutesEncoder[ContextId, ContextRef, Context](base) {

  implicit def contextEncoder: Encoder[Context] = Encoder.encodeJson.contramap { context =>
    val meta = refEncoder
      .apply(ContextRef(context.id, context.rev))
      .deepMerge(idWithLinksEncoder(context.id))
      .deepMerge(
        Json.obj(
          "deprecated" -> Json.fromBoolean(context.deprecated),
          "published"  -> Json.fromBoolean(context.published)
        )
      )
    context.value.deepMerge(meta)
  }
}
