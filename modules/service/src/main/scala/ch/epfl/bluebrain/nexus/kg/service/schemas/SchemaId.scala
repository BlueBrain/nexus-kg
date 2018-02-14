package ch.epfl.bluebrain.nexus.kg.service.schemas

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectId
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex

final case class SchemaId(projectId: ProjectId, name: String)

object SchemaId {
  private val regex: Regex     = s"${ProjectId.regex.regex}/([a-zA-Z0-9]+)".r
  private val regexName: Regex = "([a-zA-Z0-9]+)".r

  final def apply(projectId: ProjectId, name: String): Option[SchemaId] =
    name match {
      case regexName(id) => Some(new SchemaId(projectId, id))
      case _             => None
    }

  final def apply(value: String): Option[SchemaId] = value match {
    case regex(projectId, name) => ProjectId(projectId).map(new SchemaId(_, name))
    case _                      => None
  }

  final implicit val schemaIdShow: Show[SchemaId] = Show.show {
    case SchemaId(projectId, name) => s"${projectId.value}/$name"
  }

  final implicit val schemaIdIdEncoder: Encoder[SchemaId] = Encoder.encodeString.contramap(_.show)
  final implicit val projectIdDecoder: Decoder[SchemaId] =
    Decoder.decodeString.emap(apply(_).toRight("Unable to decode value into a SchemaId"))

}
