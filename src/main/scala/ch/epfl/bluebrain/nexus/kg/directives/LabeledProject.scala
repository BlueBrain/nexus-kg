package ch.epfl.bluebrain.nexus.kg.directives

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A project wrapped with its [[ProjectLabel]] information
  *
  * @param label      the project reference information,
  *                   containing the project label and organization label
  * @param project    the project
  * @param accountRef the account reference
  */
final case class LabeledProject(label: ProjectLabel, project: Project, accountRef: AccountRef) {

  /**
    * @return the project reference
    */
  def ref: ProjectRef = ProjectRef(project.uuid)

  /**
    * @return the project base used to generate IDs
    */
  def base: AbsoluteIri = project.base
}
