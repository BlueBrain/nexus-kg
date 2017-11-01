package ch.epfl.bluebrain.nexus.kg.core.organizations

import io.circe.Json
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta

/**
  * Enumeration type for all events that are emitted for organizations.
  */
sealed trait OrgEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the organization
    */
  def id: OrgId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object OrgEvent {

  /**
    * Evidence that an organization has been created.
    *
    * @param id    the unique identifier of the organization
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the initial value of the organization
    */
  final case class OrgCreated(id: OrgId, rev: Long, meta: Meta, value: Json) extends OrgEvent

  /**
    * Evidence that an organization has been updated.
    *
    * @param id    the unique identifier of the organization
    * @param rev   the revision number that this event generates
    * @param meta  the metadata associated to this event
    * @param value the new value of the organization
    */
  final case class OrgUpdated(id: OrgId, rev: Long, meta: Meta, value: Json) extends OrgEvent

  /**
    * Evidence that an organization has been deprecated.
    *
    * @param id   the unique identifier of the organization
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class OrgDeprecated(id: OrgId, rev: Long, meta: Meta) extends OrgEvent

}
