package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives.{
  complete,
  extractCredentials,
  handleExceptions,
  handleRejections,
  onSuccess,
  pathPrefix
}
import akka.http.scaladsl.server.RouteConcatenation._
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Route}
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.{OrderedKeys, jsonLdMarshaller}
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Contexts, JenaExpander}
import ch.epfl.bluebrain.nexus.kg.core.queries.{JenaJsonLdFormat, JsonLdFormat}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import io.circe.Encoder

import scala.concurrent.{ExecutionContext, Future}

abstract class DefaultRouteHandling(contexts: Contexts[Future])(implicit prefixes: PrefixUris, ec: ExecutionContext) {

  private val expander = new JenaExpander[Future](contexts)

  protected def writeRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  protected def readRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  protected def searchRoutes(implicit credentials: Option[OAuth2BearerToken]): Route

  /**
    * Combining ''resourceRoutes'' with ''searchRoutes''
    * and add rejection and exception handling to it.
    *
    * @param initialPrefix the initial prefix to be consumed
    */
  def combinedRoutesFor(initialPrefix: String): Route =
    handleExceptions(ExceptionHandling.exceptionHandler(prefixes.ErrorContext)) {
      handleRejections(RejectionHandling.rejectionHandler(prefixes.ErrorContext)) {
        pathPrefix(initialPrefix) {
          extractCredentials {
            case Some(c: OAuth2BearerToken) => combine(Some(c))
            case Some(_)                    => reject(AuthorizationFailedRejection)
            case _                          => combine(None)
          }
        }
      }
    }

  private def combine(cred: Option[OAuth2BearerToken]) =
    writeRoutes(cred) ~ readRoutes(cred) ~ searchRoutes(cred)

  /**
    * Serializes and formats an entity according to the given JSON-LD format via implicitly available instances
    * of Encoder and ToEntityMarshaller.
    *
    * @param entity the entity to be serialized
    * @param format the JSON-LD output format
    * @tparam A a generic type parameter for the entity
    * @return an Akka HTTP Route
    */
  protected def formatOutput[A](entity: A, format: JsonLdFormat)(implicit encoder: Encoder[A],
                                                                 marshaller: ToEntityMarshaller[A],
                                                                 keys: OrderedKeys): Route = {
    format match {
      case JsonLdFormat.Default => complete(StatusCodes.OK -> entity)
      case f: JenaJsonLdFormat =>
        onSuccess(expander(encoder(entity), f.toJena)) { formatted =>
          complete(StatusCodes.OK -> formatted)
        }
    }
  }

}
