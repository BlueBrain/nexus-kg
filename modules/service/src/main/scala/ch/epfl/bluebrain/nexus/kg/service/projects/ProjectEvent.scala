package ch.epfl.bluebrain.nexus.kg.service.projects

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent._
import ch.epfl.bluebrain.nexus.kg.service.types.Revisioned
import io.circe.Json
import shapeless.Typeable

/**
  * Enumeration type for all events that are emitted for projects.
  */
sealed trait ProjectEvent extends Product with Serializable with Revisioned {

  /**
    * @return the unique identifier of the project
    */
  def id: ProjectId

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object ProjectEvent {

  final case class Value(context: Json, config: Project.Config)

  private val valueType = implicitly[Typeable[Value]]

  /**
    * Evidence that a project has been created.
    *
    * @param id         the unique identifier of the project
    * @param rev        the revision number that this event generates
    * @param meta       the metadata associated to this event
    * @param `@context` the json prefix mappings of the project
    * @param config     the configuration of the project
    */
  final case class ProjectCreated(id: ProjectId, rev: Long, meta: Meta, `@context`: Json, config: Project.Config)
      extends ProjectEvent

  /**
    * Evidence that a project has been updated.
    *
    * @param id         the unique identifier of the project
    * @param rev        the revision number that this event generates
    * @param meta       the metadata associated to this event
    * @param `@context` the json prefix mappings of the project
    * @param config     the configuration of the project
    */
  final case class ProjectUpdated(id: ProjectId, rev: Long, meta: Meta, `@context`: Json, config: Project.Config)
      extends ProjectEvent

  /**
    * Evidence that a project has been deprecated.
    *
    * @param id   the unique identifier of the project
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class ProjectDeprecated(id: ProjectId, rev: Long, meta: Meta) extends ProjectEvent

  implicit def toResourceEvent(projectEvent: ProjectEvent): ResourceEvent[ProjectId] = projectEvent match {
    case ProjectCreated(id, rev, meta, context, config) => ResourceCreated(id, rev, meta, Value(context, config))
    case ProjectUpdated(id, rev, meta, context, config) => ResourceUpdated(id, rev, meta, Value(context, config))
    case ProjectDeprecated(id, rev, meta)               => ResourceDeprecated(id, rev, meta)
  }

  implicit def fromResourceEvent(res: ResourceEvent[ProjectId]): Option[ProjectEvent] = res match {
    case ResourceCreated(id, rev, meta, value) =>
      valueType.cast(value).map { case Value(context, config) => ProjectCreated(id, rev, meta, context, config) }
    case ResourceUpdated(id, rev, meta, value) =>
      valueType.cast(value).map { case Value(context, config) => ProjectUpdated(id, rev, meta, context, config) }
    case ResourceDeprecated(id, rev, meta) => Some(ProjectDeprecated(id, rev, meta))

  }

}
