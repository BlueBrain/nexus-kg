package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId}
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organization}
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId}

/**
  * Mapping form an ''Entity'' to it's ''Id''
  *
  * @tparam Entity the type of the Entity
  * @tparam Id     the type of the Id
  */
trait EntityToIdRetrieval[Entity, Id] extends (Entity => Id)

object EntityToIdRetrieval {

  implicit val instanceIdRetrieval = new EntityToIdRetrieval[Instance, InstanceId] {
    override def apply(value: Instance): InstanceId = value.id
  }
  implicit val schemaIdRetrieval = new EntityToIdRetrieval[Schema, SchemaId] {
    override def apply(value: Schema): SchemaId = value.id
  }
  implicit val shapeIdRetrieval = new EntityToIdRetrieval[Shape, ShapeId] {
    override def apply(value: Shape): ShapeId = value.id
  }
  implicit val contextIdRetrieval = new EntityToIdRetrieval[Context, ContextId] {
    override def apply(value: Context): ContextId = value.id
  }
  implicit val domainIdRetrieval = new EntityToIdRetrieval[Domain, DomainId] {
    override def apply(value: Domain): DomainId = value.id
  }
  implicit val orgIdRetrieval = new EntityToIdRetrieval[Organization, OrgId] {
    override def apply(value: Organization): OrgId = value.id
  }

}
