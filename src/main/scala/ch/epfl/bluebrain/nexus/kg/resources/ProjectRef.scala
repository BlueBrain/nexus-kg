package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.Show
import cats.syntax.show._
import io.circe.Encoder

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
}
