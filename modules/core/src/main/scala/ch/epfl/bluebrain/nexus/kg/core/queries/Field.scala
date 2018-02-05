package ch.epfl.bluebrain.nexus.kg.core.queries

import cats.Show

/**
  * Type representing the field to return from a query
  *
  * @param value the field value
  */
final case class Field(value: String)

object Field {
  val Empty                           = Field("")
  val All                             = Field("all")
  implicit val showField: Show[Field] = Show.show(_.value)

  implicit def fromString(value: String): Field = Field(value)
}
