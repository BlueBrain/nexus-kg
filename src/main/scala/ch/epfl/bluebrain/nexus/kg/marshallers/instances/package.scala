package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.marshalling.GenericMarshallers.eitherMarshaller
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
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
  final implicit def http[A](implicit ctx: AbsoluteIri,
                             encoder: Encoder[A],
                             printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                             keys: OrderedKeys = OrderedKeys(
                               List("@context", "@type", "@id", "code", "message", "details", "")))
    : ToEntityMarshaller[A] =
    jsonLd.compose(encoder.mapJson(_.addContext(ctx)).apply)

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
                           List("@context", "@type", "@id", "code", "message", "details", "")))
    : ToResponseMarshaller[Rejection] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { rejection =>
      HttpResponse(
        status = statusCodeFrom(rejection),
        entity = HttpEntity(RdfMediaTypes.`application/ld+json`, printer.pretty(rejectionEncoder(rejection).sortKeys)))
    }
}
