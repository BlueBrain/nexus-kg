package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: String)

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.show { ref =>
    s"ProjectRef(${ref.id}) "
  }
}
