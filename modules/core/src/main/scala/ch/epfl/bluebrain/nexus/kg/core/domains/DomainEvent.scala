package ch.epfl.bluebrain.nexus.kg.core.domains

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta

/**
  * Enumeration type for all events that are emitted for domains.
  */
sealed trait DomainEvent extends Product with Serializable {

  /**
    * @return the unique identifier of the domain
    */
  def id: DomainId

  /**
    * @return the revision number that this event generates
    */
  def rev: Long

  /**
    * @return the metadata associated to this event
    */
  def meta: Meta
}

object DomainEvent {

  /**
    * Evidence that a domain has been created.
    *
    * @param id          the unique identifier of the domain
    * @param rev         the revision number that this event generates
    * @param meta        the metadata associated to this event
    * @param description a description of the domain
    */
  final case class DomainCreated(id: DomainId, rev: Long, meta: Meta, description: String) extends DomainEvent

  /**
    * Evidence that a domain has been deprecated.
    *
    * @param id   the unique identifier of the domain
    * @param rev  the revision number that this event generates
    * @param meta the metadata associated to this event
    */
  final case class DomainDeprecated(id: DomainId, rev: Long, meta: Meta) extends DomainEvent

}
