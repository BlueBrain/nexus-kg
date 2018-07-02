package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.javadsl.server.AuthorizationFailedRejection
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server._
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.marshallerHttp
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.{MethodNotSupported, UnauthorizedAccess, WrongOrInvalidJson}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.CustomAuthRejection
import ch.epfl.bluebrain.nexus.kg.marshallers.ResourceJsonLdCirceSupport.statusCodeFrom
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{IllegalParameter, MissingParameter}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._

/**
  * A rejection encapsulates a specific reason why a route was not able to handle a request.
  * Rejections are gathered up over the course of a Route evaluation and finally
  * converted to CommonRejections case classes if there was no way for the request to be completed.
  */
object RejectionHandling {

  private implicit val errorContext: ContextUri = errorCtxUri

  /**
    * Defines the custom handling of rejections. When multiple rejections are generated
    * in the routes evaluation process, the priority order to handle them is defined
    * by the order of appearance in this method.
    */
  final def rejectionHandler(): RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case rej: Rejection =>
          complete(statusCodeFrom(rej) -> rej)
        case MalformedQueryParamRejection(_, _, Some(e: HttpRejection)) =>
          complete(BadRequest -> e)
        case MalformedQueryParamRejection(_, _, Some(err)) =>
          complete(BadRequest -> (IllegalParameter(err.getMessage): Rejection))
        case ValidationRejection(_, Some(err: IllegalParameter)) =>
          complete(BadRequest -> (IllegalParameter(err.getMessage): Rejection))
        case ValidationRejection(err, _) =>
          complete(BadRequest -> (IllegalParameter(err): Rejection))
        case MissingQueryParamRejection(param) =>
          complete(BadRequest -> (MissingParameter(s"'$param' parameter is required"): Rejection))
        case CustomAuthRejection(e) =>
          complete(InternalServerError -> (e: Rejection))
        case _: AuthorizationFailedRejection =>
          complete(Unauthorized -> (UnauthorizedAccess: HttpRejection))
      }
      .handleAll[MalformedRequestContentRejection] { rejection =>
        val aggregate = rejection.map(_.message).mkString(", ")
        complete(BadRequest -> (WrongOrInvalidJson(Some(aggregate)): HttpRejection))
      }
      .handleAll[MethodRejection] { methodRejections =>
        val names = methodRejections.map(_.supported.name)
        complete(MethodNotAllowed -> (MethodNotSupported(names): HttpRejection))
      }
      .result()

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  private implicit val rejectionConfig: Configuration = Configuration.default.withDiscriminator("code")

}
