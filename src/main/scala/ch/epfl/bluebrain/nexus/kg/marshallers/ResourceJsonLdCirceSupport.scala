package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.marshalling.GenericMarshallers.eitherMarshaller
import akka.http.scaladsl.marshalling.PredefinedToResponseMarshallers._
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.http.{JsonLdCirceSupport, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import io.circe.{Encoder, Json, Printer}

trait ResourceJsonLdCirceSupport extends JsonLdCirceSupport {

  // TODO: Extra information of rejection missing (code, ref, ...)
  val rejectionEncoder: Encoder[Rejection] = Encoder { rejection =>
    Json.obj("message" -> Json.fromString(rejection.msg)).addContext(errorCtxUri)
  }

  private[marshallers] def statusCodeFrom(rejection: Rejection): StatusCode = rejection match {
    case _: IsDeprecated | _: ProjectIsDeprecated | _: UpdateSchemaTypes | _: IncorrectTypes | _: IllegalContextValue |
        _: UnableToSelectResourceId | _: InvalidResource | _: IncorrectId | _: InvalidPayload | _: IllegalParameter |
        _: MissingParameter =>
      StatusCodes.BadRequest
    case _: UnexpectedState | _: Unexpected                       => StatusCodes.InternalServerError
    case _: NotFound | _: AttachmentNotFound | _: ProjectNotFound => StatusCodes.NotFound
    case _: IncorrectRev | _: AlreadyExists                       => StatusCodes.Conflict
    case _: DownstreamServiceError                                => StatusCodes.BadGateway
  }

  final implicit def rejectionToResponseMarshaller(
      implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true)): ToResponseMarshaller[Rejection] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { rejection =>
      HttpResponse(
        status = statusCodeFrom(rejection),
        entity = HttpEntity(RdfMediaTypes.`application/ld+json`, printer.pretty(rejectionEncoder(rejection))))
    }

  implicit final def resourceResponseMarshaller[A](
      implicit m: ToEntityMarshaller[A],
      printer: Printer = Printer.noSpaces.copy(dropNullValues = true)
  ): ToResponseMarshaller[Either[Rejection, A]] =
    eitherMarshaller
}

object ResourceJsonLdCirceSupport extends ResourceJsonLdCirceSupport
