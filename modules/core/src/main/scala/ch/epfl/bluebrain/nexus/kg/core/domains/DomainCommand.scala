package ch.epfl.bluebrain.nexus.kg.core.domains

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta

/**
  * Enumeration type for commands that apply to domains.
  */
sealed trait DomainCommand extends Product with Serializable {

  /**
    * @return the unique identifier for the domain for which this command will be evaluated
    */
  def id: DomainId

  /**
    * @return the metadata associated to this command
    */
  def meta: Meta
}

object DomainCommand {

  /**
    * Command that signals the intent to create a new domain.
    *
    * @param id          the unique identifier for the organization to be created
    * @param meta        the metadata associated to this command
    * @param description a description of the domain
    */
  final case class CreateDomain(id: DomainId, meta: Meta, description: String) extends DomainCommand

  /**
    * Command that signals the intent to deprecate an existing domain.
    *
    * @param id   the unique identifier for the domain to be deprecated
    * @param rev  the last known revision of the domain
    * @param meta the metadata associated to this command
    */
  final case class DeprecateDomain(id: DomainId, rev: Long, meta: Meta) extends DomainCommand

}
