package ch.epfl.bluebrain.nexus.kg.core.types

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.types.Project.ProjectValue
import io.circe.Json

/**
  * Data type representing the state of a project.
  * TODO: This is the same as in ADMIN and it needs to be placed in admin-client module
  *
  * @param id         a identifier for the project
  * @param rev        the selected revision for the project
  *
  * @param value      the payload of the project
  * @param deprecated the deprecation status of the project
  */
final case class Project(id: String, rev: Long, value: ProjectValue, original: Json, deprecated: Boolean)

object Project {

  /**
    * Data type representing the payload value of the project
    *
    * @param name           the name of the project
    * @param description    the optionally available description
    * @param prefixMappings the prefix mappings
    * @param config         the configuration of the project
    */
  final case class ProjectValue(name: String,
                                description: Option[String],
                                prefixMappings: List[LoosePrefixMapping],
                                config: Config)

  /**
    * A single name to uri mapping (an entry of a prefix mapping). This contains prefix mappings and aliases
    *
    * @param prefix    the prefix (left side of a PrefixMapping)
    * @param namespace the namespace or the alias value (right side of a PrefixMapping)
    */
  final case class LoosePrefixMapping(prefix: String, namespace: Uri)

  /**
    * Project configuration
    *
    * @param maxAttachmentSize the maximum attachment file size in bytes
    */
  final case class Config(maxAttachmentSize: Long)
}
