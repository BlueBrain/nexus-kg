package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show

/**
  * Representation of the project label, containing both the organization and the project segments
  *
  * @param organization the organization segment of the label
  * @param value        the project segment of the label
  */
final case class ProjectLabel(organization: String, value: String)

object ProjectLabel {
  implicit val segmentShow: Show[ProjectLabel] = Show.show(s => s"${s.organization}/${s.value}")
}
