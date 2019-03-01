package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.Show
import cats.syntax.show._
import io.circe.{Decoder, Encoder}

import scala.util.Try

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: UUID)

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.show(_.id.toString)
  final implicit val projectRefEncoder: Encoder[ProjectRef] =
    Encoder.encodeString.contramap(_.show)
  final implicit val projectRefDecoder: Decoder[ProjectRef] =
    Decoder.decodeString.emapTry(uuid => Try(UUID.fromString(uuid)).map(ProjectRef.apply))
}
