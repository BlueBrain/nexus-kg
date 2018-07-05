package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.marshalling.GenericMarshallers.eitherMarshaller
import akka.http.scaladsl.marshalling.PredefinedToResponseMarshallers._
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.http.{JsonLdCirceSupport, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import io.circe.Printer

trait ResourceJsonLdCirceSupport extends JsonLdCirceSupport {

  final implicit def rejectionToResponseMarshaller(
      implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
      ordered: OrderedKeys = OrderedKeys(List("@context", "code", "message", "details", "")))
    : ToResponseMarshaller[Rejection] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { rejection =>
      HttpResponse(
        status = statusCodeFrom(rejection),
        entity = HttpEntity(RdfMediaTypes.`application/ld+json`, printer.pretty(rejectionEncoder(rejection).sortKeys)))
    }

  implicit final def resourceResponseMarshaller[A](
      implicit m: ToEntityMarshaller[A],
      printer: Printer = Printer.noSpaces.copy(dropNullValues = true)
  ): ToResponseMarshaller[Either[Rejection, A]] =
    eitherMarshaller
}

object ResourceJsonLdCirceSupport extends ResourceJsonLdCirceSupport
