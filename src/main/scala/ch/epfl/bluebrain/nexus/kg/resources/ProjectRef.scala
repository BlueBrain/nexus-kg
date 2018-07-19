package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: String) extends AnyVal

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.fromToString
}
