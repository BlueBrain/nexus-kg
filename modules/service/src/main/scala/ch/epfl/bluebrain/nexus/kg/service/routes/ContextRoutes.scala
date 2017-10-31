package ch.epfl.bluebrain.nexus.kg.service.routes
import java.time.Clock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.commons.iam.IamClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.CannotUnpublishSchema
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.service.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.service.directives.ResourceDirectives._
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.Publish
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives.traceName

import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for context specific functionality
  *
  * @param contexts  the context operation bundle
  * @param base      the service public uri + prefix
  * @param iamClient IAM client
  * @param ec        execution context
  * @param clock     the clock used to issue instants
  */
class ContextRoutes(contexts: Contexts[Future], base: Uri)(implicit iamClient: IamClient[Future],
                                                           ec: ExecutionContext,
                                                           clock: Clock)
    extends DefaultRouteHandling {

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

  override protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]) = reject()

  def routes: Route = combinedRoutesFor("contexts")
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
  final def apply(contexts: Contexts[Future],
                  base: Uri)(implicit iamClient: IamClient[Future], ec: ExecutionContext, clock: Clock): ContextRoutes =
    new ContextRoutes(contexts, base)

  /**
    * Local context config definition that models a resource for context state change operations
    * @param published whether the context should be published or un-published
    */
  final case class ContextConfig(published: Boolean)

}

class ContextCustomEncoders(base: Uri) extends RoutesEncoder[ContextId, ContextRef](base) {

  implicit def contextEncoder: Encoder[Context] = Encoder.encodeJson.contramap { context =>
    val meta = refEncoder
      .apply(ContextRef(context.id, context.rev))
      .deepMerge(
        Json.obj(
          "deprecated" -> Json.fromBoolean(context.deprecated),
          "published"  -> Json.fromBoolean(context.published)
        )
      )
    context.value.deepMerge(meta)
  }
}
