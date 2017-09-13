package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import akka.http.scaladsl.model._
import io.circe.Encoder

/**
  * Directive to marshall StatusFrom instances into HTTP responses.
  */
trait ErrorDirectives {

  /**
    * Implicitly derives a [[akka.http.scaladsl.marshalling.ToResponseMarshaller]] for any type ''A'' based on
    * implicitly available [[ch.epfl.bluebrain.nexus.kg.service.directives.StatusFrom]] and [[io.circe.Encoder]]
    * instances.
    *
    * @tparam A the generic type for which the marshaller is derived
    * @return a ''ToResponseMarshaller'' that will generate a response using the status provided by ''StatusFrom'' and
    *         the entity using the json representation provided by ''Encoder''
    */
  final implicit def marshallerFromStatusAndEncoder[A: StatusFrom : Encoder]: ToResponseMarshaller[A] =
    Marshaller.withFixedContentType(ContentTypes.`application/json`) { value =>
      HttpResponse(
        status = implicitly[StatusFrom[A]].apply(value),
        entity = HttpEntity(ContentTypes.`application/json`, implicitly[Encoder[A]].apply(value).noSpaces))
    }
}

object ErrorDirectives extends ErrorDirectives