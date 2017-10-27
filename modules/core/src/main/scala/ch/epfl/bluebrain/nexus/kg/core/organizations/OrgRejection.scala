package ch.epfl.bluebrain.nexus.kg.core.organizations

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait OrgRejection extends Rejection

object OrgRejection {

  /**
    * Signals that an organization cannot be created because one with the same identifier already exists.
    */
  final case object OrgAlreadyExists extends OrgRejection

  /**
    * Signals that an operation on an organization cannot be performed due to the fact that the referenced organization
    * does not exists.
    */
  final case object OrgDoesNotExist extends OrgRejection

  /**
    * Signals that an organization update cannot be performed due its deprecation status.
    */
  final case object OrgIsDeprecated extends OrgRejection

  /**
    * Signals that an organization update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends OrgRejection

  /**
    * Signals the failure to create a new organization due to an invalid ''id'' provided.
    *
    * @param id the provided identifier
    */
  final case class InvalidOrganizationId(id: String) extends OrgRejection

}