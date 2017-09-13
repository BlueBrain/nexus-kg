package ch.epfl.bluebrain.nexus.kg.core.organizations

import io.circe.Json

/**
  * Enumeration type for commands that apply to organizations.
  */
sealed trait OrgCommand extends Product with Serializable {
  /**
    * @return the unique identifier for the organization for which this command will be evaluated
    */
  def id: OrgId
}

object OrgCommand {

  /**
    * Command that signals the intent to create a new organization.
    *
    * @param id    the unique identifier for the organization to be created
    * @param value a json representation of the organization
    */
  final case class CreateOrg(id: OrgId, value: Json) extends OrgCommand

  /**
    * Command that signals the intent to update an existing organization.
    *
    * @param id    the unique identifier for the organization to be created
    * @param rev   the last known revision of the organization
    * @param value the new json value for the organization
    */
  final case class UpdateOrg(id: OrgId, rev: Long, value: Json) extends OrgCommand

  /**
    * Command that signals the intent to deprecate an existing organization.
    *
    * @param id  the unique identifier for the organization to be deprecated
    * @param rev the last known revision of the organization
    */
  final case class DeprecateOrg(id: OrgId, rev: Long) extends OrgCommand

}