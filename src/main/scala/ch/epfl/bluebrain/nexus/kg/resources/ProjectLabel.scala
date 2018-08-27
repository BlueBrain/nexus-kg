package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.admin.refined.project.ProjectReference
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path._
import ch.epfl.bluebrain.nexus.service.http.UriOps._

final case class ProjectLabel(account: String, value: String) {

  /**
    * @return the project accessId
    */
  def projectAccessId(implicit config: AdminConfig): AbsoluteIri =
    url"${config.baseUri.append("projects" / account / value)}"
}

object ProjectLabel {
  implicit val segmentShow: Show[ProjectLabel] = Show.show(s => s"${s.account}/${s.value}")
  implicit def toProjectLabel(ref: ProjectReference): ProjectLabel =
    ProjectLabel(ref.organizationReference.value, ref.projectLabel.value)

}
