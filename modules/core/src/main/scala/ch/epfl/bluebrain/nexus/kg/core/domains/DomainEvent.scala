package ch.epfl.bluebrain.nexus.kg.core.domains

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
}

object DomainEvent {

  /**
    * Evidence that a domain has been created.
    *
    * @param id          the unique identifier of the domain
    * @param rev         the revision number that this event generates
    * @param description a description of the domain
    */
  final case class DomainCreated(id: DomainId, rev: Long, description: String) extends DomainEvent

  /**
    * Evidence that a domain has been deprecated.
    *
    * @param id  the unique identifier of the domain
    * @param rev the revision number that this event generates
    */
  final case class DomainDeprecated(id: DomainId, rev: Long) extends DomainEvent

}
