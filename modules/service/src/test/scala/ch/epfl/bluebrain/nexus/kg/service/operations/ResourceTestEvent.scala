package ch.epfl.bluebrain.nexus.kg.service.operations

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent.{
  ResourceCreated,
  ResourceDeprecated,
  ResourceUpdated
}
import ch.epfl.bluebrain.nexus.kg.service.types.Revisioned
import io.circe.Json
import shapeless.Typeable

sealed trait ResourceTestEvent extends Product with Serializable with Revisioned {
  def id: Id
  def meta: Meta
}

object ResourceTestEvent {

  private val jsonType = implicitly[Typeable[Json]]

  final case class ResourceTestCreated(id: Id, rev: Long, meta: Meta, value: Json) extends ResourceTestEvent

  final case class ResourceTestUpdated(id: Id, rev: Long, meta: Meta, value: Json) extends ResourceTestEvent

  final case class ResourceTestDeprecated(id: Id, rev: Long, meta: Meta) extends ResourceTestEvent

  implicit def toResourceEvent(event: ResourceTestEvent): ResourceEvent[Id] = event match {
    case ResourceTestCreated(id, rev, meta, value) => ResourceCreated(id, rev, meta, value)
    case ResourceTestUpdated(id, rev, meta, value) => ResourceUpdated(id, rev, meta, value)
    case ResourceTestDeprecated(id, rev, meta)     => ResourceDeprecated(id, rev, meta)
  }

  implicit def fromResourceEvent(event: ResourceEvent[Id]): Option[ResourceTestEvent] = event match {
    case ResourceCreated(id, rev, meta, value) => jsonType.cast(value).map(ResourceTestCreated(id, rev, meta, _))
    case ResourceUpdated(id, rev, meta, value) => jsonType.cast(value).map(ResourceTestUpdated(id, rev, meta, _))
    case ResourceDeprecated(id, rev, meta)     => Some(ResourceTestDeprecated(id, rev, meta))

  }

}
