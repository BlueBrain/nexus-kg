package ch.epfl.bluebrain.nexus.kg.directives

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.admin.refined.project.ProjectReference

/**
  * A project wrapped with its [[ProjectReference]] information
  *
  * @param label   the project reference information,
  *                containing the project label and organization label
  * @param project the project
  */
final case class LabeledProject(label: ProjectReference, project: Project)
