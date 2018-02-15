package ch.epfl.bluebrain.nexus.kg.service.projects

import ch.epfl.bluebrain.nexus.kg.service.projects.Project.Config
import io.circe.Json

final case class Project(id: ProjectId, rev: Long, `@context`: Json, config: Config, deprecated: Boolean)

object Project {
  final case class Config(maxAttachmentSize: Long)
}
