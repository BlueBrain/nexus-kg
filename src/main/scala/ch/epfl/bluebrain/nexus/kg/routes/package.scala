package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.resources._

package object routes {

  private[routes] implicit def toProject(implicit value: LabeledProject): Project           = value.project
  private[routes] implicit def toProjectLabel(implicit value: LabeledProject): ProjectLabel = value.label

}
