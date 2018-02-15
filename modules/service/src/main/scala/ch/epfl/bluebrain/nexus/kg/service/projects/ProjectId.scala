package ch.epfl.bluebrain.nexus.kg.service.projects

import cats.Show
import ch.epfl.bluebrain.nexus.kg.service.types.Named
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex

final case class ProjectId(name: String) extends Named

object ProjectId {
  val regex: Regex = "([a-zA-Z0-9]+)".r

  final def apply(value: String): Option[ProjectId] = value match {
    case regex(id) => Some(new ProjectId(id))
    case _         => None
  }

  final implicit val projectIdShow: Show[ProjectId]       = Show.show(_.name)
  final implicit val projectIdEncoder: Encoder[ProjectId] = Encoder.encodeString.contramap(_.name)
  final implicit val projectIdDecoder: Decoder[ProjectId] =
    Decoder.decodeString.emap(apply(_).toRight("Unable to decode value into a ProjectId"))

}
