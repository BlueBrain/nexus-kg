package ch.epfl.bluebrain.nexus.kg.resources

import cats.Functor
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.{Encoder, Json}
import io.circe.syntax._

/**
  * Represents a value and its identifier
  *
  * @param id    the identifier of the value
  * @param value the value
  */
final case class IdentifiedValue[A](id: AbsoluteIri, value: A)

object IdentifiedValue {
  implicit def encoderIdentifiedValue[A: Encoder]: Encoder[IdentifiedValue[A]] = Encoder.instance {
    case IdentifiedValue(id, value) => Json.obj("@id" -> id.asString.asJson) deepMerge value.asJson
  }

  implicit val functorIdentifiedValue: Functor[IdentifiedValue] = new Functor[IdentifiedValue] {
    override def map[A, B](fa: IdentifiedValue[A])(f: A => B): IdentifiedValue[B] = fa.copy(value = f(fa.value))
  }
}
