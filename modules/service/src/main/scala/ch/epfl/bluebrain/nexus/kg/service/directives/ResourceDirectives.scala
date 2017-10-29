package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.{Directive1, ValidationRejection}
import akka.http.scaladsl.server.Directives._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat

/**
  * Resource specific collection of directives that match the path structure against expected resource identifier
  * structures.
  */
trait ResourceDirectives {

  /**
    * Attempts to match the next available path segment as a ''Version''.
    */
  def extractVersion: Directive1[Version] =
    pathPrefix(Segment).flatMap { verStr =>
      Version(verStr) match {
        case Some(ver) => provide(ver)
        case None =>
          reject(ValidationRejection("Illegal version format", Some(IllegalVersionFormat("Illegal version format"))))
      }
    }

  /**
    * Extracts an [[OrgId]] from the request uri path.
    */
  def extractOrgId: Directive1[OrgId] =
    pathPrefix(Segment).tmap {
      case Tuple1(org) => Tuple1(OrgId(org))
    }

  /**
    * Extracts a [[DomainId]] from the request uri path.
    */
  def extractDomainId: Directive1[DomainId] =
    (extractOrgId & pathPrefix(Segment)).tmap {
      case (orgId, dom) => Tuple1(DomainId(orgId, dom))
    }

  /**
    * Extracts a [[SchemaName]] from the request uri path.
    */
  def extractSchemaName: Directive1[SchemaName] =
    (extractDomainId & pathPrefix(Segment)).tmap {
      case (domainId, schemaName) => Tuple1(SchemaName(domainId, schemaName))
    }

  /**
    * Extracts a [[SchemaId]] from the request uri path.
    */
  def extractSchemaId: Directive1[SchemaId] =
    (extractSchemaName & extractVersion).tmap {
      case (SchemaName(domainId, schemaName), ver) => Tuple1(SchemaId(domainId, schemaName, ver))
    }

  /**
    * Extracts an [[InstanceId]] from the request uri path.
    */
  def extractInstanceId: Directive1[InstanceId] =
    (extractSchemaId & pathPrefix(Segment)).tmap {
      case (schemaId, uuid) => Tuple1(InstanceId(schemaId, uuid))
    }

  /**
    * Extracts a [[ContextName]] from the request uri path.
    */
  def extractContextName: Directive1[ContextName] =
    (extractDomainId & pathPrefix(Segment)).tmap {
      case (domainId, contextName) => Tuple1(ContextName(domainId, contextName))
    }

  /**
    * Extracts a [[ContextId]] from the request uri path.
    */
  def extractContextId: Directive1[ContextId] =
    (extractContextName & extractVersion).tmap {
      case (ContextName(domainId, contextName), ver) => Tuple1(ContextId(domainId, contextName, ver))
    }
}

object ResourceDirectives extends ResourceDirectives
