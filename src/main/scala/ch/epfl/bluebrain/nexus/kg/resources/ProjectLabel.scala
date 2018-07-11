package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show
import ch.epfl.bluebrain.nexus.admin.refined.project.ProjectReference

final case class ProjectLabel(account: String, value: String)

object ProjectLabel {
  implicit val segmentShow: Show[ProjectLabel] = Show.show(s => s"${s.account}/${s.value}")
  implicit def toProjectLabel(ref: ProjectReference): ProjectLabel =
    ProjectLabel(ref.organizationReference.value, ref.projectLabel.value)

}
