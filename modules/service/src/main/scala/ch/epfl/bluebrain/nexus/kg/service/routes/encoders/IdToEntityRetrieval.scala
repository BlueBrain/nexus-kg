package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organization, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, Schemas}
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.IdToEntityRetrieval._
import scala.concurrent.Future

/**
  * Defines the way to retrieve an ''Entity'' given an ''Id''
  *
  * @tparam Id     the type of the Id
  * @tparam Entity the type of the Entity
  */
trait IdToEntityRetrieval[Id, Entity] extends (Id => Future[Option[Entity]])

class GroupedIdsToEntityRetrieval(
    instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
    schemas: Schemas[Future],
    contexts: Contexts[Future],
    domains: Domains[Future],
    orgs: Organizations[Future]) {
  implicit val implicitInstanceIdToEntityRetrieval = instanceIdToEntityRetrieval(instances)
  implicit val implicitSchemaIdToEntityRetrieval   = schemaIdToEntityRetrieval(schemas)
  implicit val implicitShapeIdToEntityRetrieval    = shapeIdToEntityRetrieval(schemas)
  implicit val implicitContextIdToEntityRetrieval  = contextIdToEntityRetrieval(contexts)
  implicit val implicitDomainIdToEntityRetrieval   = domainIdToEntityRetrieval(domains)
  implicit val implicitOrgIdToEntityRetrieval      = orgIdToEntityRetrieval(orgs)

}

object IdToEntityRetrieval {
  def instanceIdToEntityRetrieval(
      instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]]) =
    new IdToEntityRetrieval[InstanceId, Instance] {
      override def apply(id: InstanceId): Future[Option[Instance]] = instances.fetch(id)
    }
  def schemaIdToEntityRetrieval(schemas: Schemas[Future]) = new IdToEntityRetrieval[SchemaId, Schema] {
    override def apply(id: SchemaId): Future[Option[Schema]] = schemas.fetch(id)
  }
  def shapeIdToEntityRetrieval(schemas: Schemas[Future]) = new IdToEntityRetrieval[ShapeId, Shape] {
    override def apply(id: ShapeId): Future[Option[Shape]] = schemas.fetchShape(id.schemaId, id.name)
  }
  def contextIdToEntityRetrieval(contexts: Contexts[Future]) = new IdToEntityRetrieval[ContextId, Context] {
    override def apply(id: ContextId): Future[Option[Context]] = contexts.fetch(id)
  }
  def domainIdToEntityRetrieval(domains: Domains[Future]) = new IdToEntityRetrieval[DomainId, Domain] {
    override def apply(id: DomainId): Future[Option[Domain]] = domains.fetch(id)
  }
  def orgIdToEntityRetrieval(orgs: Organizations[Future]) = new IdToEntityRetrieval[OrgId, Organization] {
    override def apply(id: OrgId): Future[Option[Organization]] = orgs.fetch(id)
  }
}
