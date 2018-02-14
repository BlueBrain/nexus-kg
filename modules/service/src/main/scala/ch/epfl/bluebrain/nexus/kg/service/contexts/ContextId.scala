package ch.epfl.bluebrain.nexus.kg.service.contexts

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectId
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex

final case class ContextId(projectId: ProjectId, name: String)

object ContextId {
  private val regex: Regex     = s"${ProjectId.regex.regex}/([a-zA-Z0-9]+)".r
  private val regexName: Regex = "([a-zA-Z0-9]+)".r

  final def apply(projectId: ProjectId, name: String): Option[ContextId] =
    name match {
      case regexName(id) => Some(new ContextId(projectId, id))
      case _             => None
    }
  final def apply(value: String): Option[ContextId] = value match {
    case regex(projectId, name) => ProjectId(projectId).map(new ContextId(_, name))
    case _                      => None
  }

  final implicit val schemaIdShow: Show[ContextId] = Show.show {
    case ContextId(projectId, name) => s"${projectId.value}/$name"
  }

  final implicit val schemaIdIdEncoder: Encoder[ContextId] = Encoder.encodeString.contramap(_.show)
  final implicit val projectIdDecoder: Decoder[ContextId] =
    Decoder.decodeString.emap(apply(_).toRight("Unable to decode value into a ContextId"))

}
