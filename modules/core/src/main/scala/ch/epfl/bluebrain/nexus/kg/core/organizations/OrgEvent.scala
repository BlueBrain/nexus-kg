package ch.epfl.bluebrain.nexus.kg.core.organizations

import io.circe.Json

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
}

object OrgEvent {

  /**
    * Evidence that an organization has been created.
    *
    * @param id    the unique identifier of the organization
    * @param rev   the revision number that this event generates
    * @param value the initial value of the organization
    */
  final case class OrgCreated(id: OrgId, rev: Long, value: Json) extends OrgEvent

  /**
    * Evidence that an organization has been updated.
    *
    * @param id    the unique identifier of the organization
    * @param rev   the revision number that this event generates
    * @param value the new value of the organization
    */
  final case class OrgUpdated(id: OrgId, rev: Long, value: Json) extends OrgEvent

  /**
    * Evidence that an organization has been deprecated.
    *
    * @param id  the unique identifier of the organization
    * @param rev the revision number that this event generates
    */
  final case class OrgDeprecated(id: OrgId, rev: Long) extends OrgEvent

}