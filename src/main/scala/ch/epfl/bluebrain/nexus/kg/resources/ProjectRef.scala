package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.Show

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: UUID)

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.show(_.id.toString)
}
