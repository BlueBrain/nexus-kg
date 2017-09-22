package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.{Directive1, ValidationRejection}
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat
import shapeless.ops.coproduct.Selector
import shapeless.{:+:, CNil, Coproduct}

import scala.annotation.implicitNotFound

/**
  * Collection of path specific directives.
  */
trait PathDirectives {

  /**
    * Extracts the ''version'' path parameter and attempts to convert
    * it into a [[Version]]
    */
  def versioned: Directive1[Version] =
    pathPrefix(Segment).flatMap { versionString =>
      Version(versionString) match {
        case None          =>
          reject(ValidationRejection("Illegal version format", Some(IllegalVersionFormat("Illegal version format"))))
        case Some(version) =>
          provide(version)
      }
    }

  type ResourceId = InstanceId :+: SchemaId :+: SchemaName :+: DomainId :+: OrgId :+: None.type :+: CNil
  type ResourceIdSelector[A] = Selector[ResourceId, A]

  /**
    * Extracts all the choices of ''resourceId''s from the path parameter
    * with the expected API structure: {org}/{domain}/{schema_name}/{schema_version}/{instance_uuid}
    *
    * @param max the maximum number of [[Segment]] to be consumed
    */
  def extractAnyResourceId(max: Int = 5): Directive1[ResourceId] =
    pathPrefix(Segments(0, max)).flatMap {
      case Nil                                            => provide(Coproduct[ResourceId](None))
      case org :: Nil                                     => provide(Coproduct[ResourceId](OrgId(org)))
      case org :: dom :: Nil                              => provide(Coproduct[ResourceId](DomainId(OrgId(org), dom)))
      case org :: dom :: schema :: Nil                    => provide(Coproduct[ResourceId](SchemaName(DomainId(OrgId(org), dom), schema)))
      case org :: dom :: schema :: version :: Nil         => Version(version) match {
        case Some(ver) =>
          provide(Coproduct[ResourceId](SchemaId(DomainId(OrgId(org), dom), schema, ver)))
        case None      =>
          reject(ValidationRejection("Illegal version format", Some(IllegalVersionFormat("Illegal version format"))))
      }
      case org :: dom :: schema :: version :: uuid :: Nil => Version(version) match {
        case Some(ver) =>
          provide(Coproduct[ResourceId](InstanceId(SchemaId(DomainId(OrgId(org), dom), schema, ver), uuid)))
        case None      =>
          reject(ValidationRejection("Illegal version format", Some(IllegalVersionFormat("Illegal version format"))))
      }
      case _                                              => reject
    }


  /**
    * Summons a [[ResourceIdSelector]] instance from the implicit scope.
    *
    * @param sel the implicitly available [[ResourceIdSelector]] for the generic type ''A''
    * @tparam A the resourceId to be extracted. It which should be one of the types in [[ResourceId]] Coproduct
    */
  @implicitNotFound("The type param ${A} needs to be one of ['InstanceId', 'SchemaId', 'SchemaName', 'DomainId', 'OrgId', 'None.type']")
  def of[A](implicit sel: ResourceIdSelector[A]): ResourceIdSelector[A] = sel

  /**
    * Extracts the resourceId of type ''A'' from the choices generated in ''resourceId''.
    *
    * @param resourceId the choices for each type in [[ResourceId]]
    * @param selector   the selector for the type ''A''
    * @tparam A the resourceId to be extracted. It which should be one of the types in [[ResourceId]] Coproduct
    */
  def resourceId[A](resourceId: ResourceId, selector: ResourceIdSelector[A]): Directive1[A] =
    resourceId.select[A](selector) match {
      case Some(a) => provide(a)
      case None    => reject
    }

  /**
    * Extracts the resourceId of type ''A'' from the choices generated in ''extractAnyResourceId''.
    *
    * @param selector the selector for the type ''A''
    * @tparam A the resourceId to be extracted. It which should be one of the types in [[ResourceId]] Coproduct
    */
  def extractResourceId[A](depth: Int, selector: ResourceIdSelector[A]): Directive1[A] = {
    extractAnyResourceId(depth).
      flatMap(id => resourceId(id, selector))
  }
}

object PathDirectives extends PathDirectives
