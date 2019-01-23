package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.marshalling.GenericMarshallers.eitherMarshaller
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.syntax._
import io.circe.{Encoder, Json, Printer}
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.immutable.Seq
import scala.concurrent.Future

object instances extends FailFastCirceSupport {

  override def unmarshallerContentTypes: Seq[ContentTypeRange] =
    List(`application/json`, `application/ld+json`, `application/sparql-results+json`)

  /**
    * `Json` => HTTP entity
    *
    * @return marshaller for JSON-LD value
    */
  final implicit def jsonLd(implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                            keys: OrderedKeys = orderedKeys): ToEntityMarshaller[Json] = {
    val marshallers = Seq(`application/ld+json`, `application/json`).map(contentType =>
      Marshaller.withFixedContentType[Json, MessageEntity](contentType) { json =>
        HttpEntity(`application/ld+json`, printer.pretty(json.sortKeys))
    })
    Marshaller.oneOf(marshallers: _*)
  }

  /**
    * `A` => HTTP entity
    *
    * @tparam A type to encode
    * @return marshaller for any `A` value
    */
  final implicit def httpEntity[A](implicit encoder: Encoder[A],
                                   printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                                   keys: OrderedKeys = orderedKeys): ToEntityMarshaller[A] =
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
    eitherMarshaller(rejection, httpEntity[A])

  /**
    * `Rejection` => HTTP response
    *
    * @return marshaller for Rejection value
    */
  implicit def rejection(implicit printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                         ordered: OrderedKeys = orderedKeys): ToResponseMarshaller[Rejection] = {
    val marshallers = Seq(`application/ld+json`, `application/json`).map { contentType =>
      Marshaller.withFixedContentType[Rejection, HttpResponse](contentType) { rejection =>
        HttpResponse(status = statusCodeFrom(rejection),
                     entity = HttpEntity(contentType, printer.pretty(rejection.asJson.sortKeys)))
      }
    }
    Marshaller.oneOf(marshallers: _*)
  }

  implicit class EitherTask[R <: Rejection, A](task: Task[Either[R, A]])(implicit s: Scheduler) {
    def runWithStatus(code: StatusCode): Future[Either[R, (StatusCode, A)]] =
      task.map(_.map(code -> _)).runToFuture
  }

  implicit class OptionTask[A](task: Task[Option[A]])(implicit s: Scheduler) {
    def runNotFound: Future[A] =
      task.flatMap {
        case Some(a) => Task.pure(a)
        case None    => Task.raiseError(KgError.NotFound())
      }.runToFuture
  }
}
