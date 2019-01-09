package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show
import ch.epfl.bluebrain.nexus.admin.client.config.AdminClientConfig
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._

final case class ProjectLabel(organization: String, value: String) {

  /**
    * @return the project accessId
    */
  def projectAccessId(implicit config: AdminClientConfig): AbsoluteIri =
    config.baseIri + "projects" / organization / value
}

object ProjectLabel {
  implicit val segmentShow: Show[ProjectLabel] = Show.show(s => s"${s.organization}/${s.value}")
}
