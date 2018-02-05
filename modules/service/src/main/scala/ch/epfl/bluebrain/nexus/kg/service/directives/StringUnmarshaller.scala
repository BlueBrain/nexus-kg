package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.unmarshalling.{FromStringUnmarshaller, Unmarshaller}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.WrongOrInvalidJson
import io.circe.parser._
import io.circe.{Decoder, Json}

object StringUnmarshaller {

  /**
    * String => `Json`
    *
    * @return unmarshaller for `Json`
    */
  implicit val jsonFromStringUnmarshaller: FromStringUnmarshaller[Json] =
    Unmarshaller.strict[String, Json] {
      case "" => throw Unmarshaller.NoContentException
      case value =>
        parse(value) match {
          case Left(err)   => throw new WrongOrInvalidJson(Some(err.message))
          case Right(json) => json
        }
    }

  /**
    * String => `A`
    *
    * @param f the function to convert from String => `Json`
    * @return unmarshaller for `A`
    */
  def unmarshaller[A](f: (String => Either[Throwable, Json]))(
      implicit dec: Decoder[A]): FromStringUnmarshaller[A] =
    Unmarshaller.strict[String, A] {
      case "" => throw Unmarshaller.NoContentException
      case string =>
        f(string).flatMap(_.as[A]) match {
          case Right(value) => value
          case Left(err)    => throw new IllegalArgumentException(err)
        }
    }

}
