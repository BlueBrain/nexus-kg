package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.marshalling.GenericMarshallers.eitherMarshaller
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Encoder, Json, Printer}

package object instances extends FailFastCirceSupport {

  /**
    * `Json` => HTTP entity
    *
    * @return marshaller for JSON-LD value
    */
  final implicit def jsonLd(implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                            keys: OrderedKeys = OrderedKeys()): ToEntityMarshaller[Json] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { json =>
      HttpEntity(RdfMediaTypes.`application/ld+json`, printer.pretty(json.sortKeys))
    }

  /**
    * `A` => HTTP entity
    *
    * @tparam A type to encode
    * @return marshaller for any `A` value
    */
  final implicit def http[A](implicit encoder: Encoder[A],
                             printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                             keys: OrderedKeys = OrderedKeys(
                               List("@context", "@id", "@type", "code", "message", "details", "")))
    : ToEntityMarshaller[A] =
    jsonLd.compose(encoder.apply)

  /**
    * `Either[Rejection,A]` => HTTP entity
    *
    * @tparam A type to encode
    * @return marshaller for any `A` value
    */
  implicit final def either[A](
      implicit encoder: Encoder[A],
      printer: Printer = Printer.noSpaces.copy(dropNullValues = true)
  ): ToResponseMarshaller[Either[Rejection, A]] =
    eitherMarshaller(rejection, http[A])

  /**
    * `Rejection` => HTTP entity
    *
    * @return marshaller for Rejection value
    */
  implicit def rejection(implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                         ordered: OrderedKeys = OrderedKeys(
                           List("@context", "@id", "@type", "code", "message", "details", "")))
    : ToResponseMarshaller[Rejection] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { rejection =>
      HttpResponse(
        status = statusCodeFrom(rejection),
        entity = HttpEntity(RdfMediaTypes.`application/ld+json`, printer.pretty(rejectionEncoder(rejection).sortKeys)))
    }

  private def statusCodeFrom(rejection: Rejection): StatusCode = rejection match {
    case _: IsDeprecated             => StatusCodes.BadRequest
    case _: ProjectIsDeprecated      => StatusCodes.BadRequest
    case _: UpdateSchemaTypes        => StatusCodes.BadRequest
    case _: IncorrectTypes           => StatusCodes.BadRequest
    case _: IllegalContextValue      => StatusCodes.BadRequest
    case _: UnableToSelectResourceId => StatusCodes.BadRequest
    case _: InvalidResource          => StatusCodes.BadRequest
    case _: IncorrectId              => StatusCodes.BadRequest
    case _: InvalidPayload           => StatusCodes.BadRequest
    case _: IllegalParameter         => StatusCodes.BadRequest
    case _: MissingParameter         => StatusCodes.BadRequest
    case _: Unexpected               => StatusCodes.InternalServerError
    case _: UnexpectedState          => StatusCodes.InternalServerError
    case _: AttachmentNotFound       => StatusCodes.NotFound
    case _: ProjectNotFound          => StatusCodes.NotFound
    case _: NotFound                 => StatusCodes.NotFound
    case _: IncorrectRev             => StatusCodes.Conflict
    case _: AlreadyExists            => StatusCodes.Conflict
    case _: DownstreamServiceError   => StatusCodes.BadGateway
  }
}
