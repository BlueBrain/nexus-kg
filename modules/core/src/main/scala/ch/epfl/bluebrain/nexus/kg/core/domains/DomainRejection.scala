package ch.epfl.bluebrain.nexus.kg.core.domains

import ch.epfl.bluebrain.nexus.commons.types.Rejection

/**
  * Enumeration type for rejections returned when attempting to evaluate commands.
  */
sealed trait DomainRejection extends Rejection

object DomainRejection {

  /**
    * Signals that a domain cannot be created because one with the same identifier already exists.
    */
  final case object DomainAlreadyExists extends DomainRejection

  /**
    * Signals that an operation on a domain cannot be performed due to the fact that the referenced domain does not
    * exists.
    */
  final case object DomainDoesNotExist extends DomainRejection

  /**
    * Signals that a domain cannot be deprecated as it's already in this state.
    */
  final case object DomainAlreadyDeprecated extends DomainRejection

  /**
    * Signals that a domain update cannot be performed due to an incorrect revision provided.
    */
  final case object IncorrectRevisionProvided extends DomainRejection

  /**
    * Signals that a domain update cannot be performed due to its deprecation status.
    */
  final case object DomainIsDeprecated extends DomainRejection

  /**
    * Signals the failure to create a new domain due to an invalid ''id'' provided.
    *
    * @param id the provided identifier
    */
  final case class InvalidDomainId(id: String) extends DomainRejection
}