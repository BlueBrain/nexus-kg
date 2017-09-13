package ch.epfl.bluebrain.nexus.kg.core.domains

/**
  * Enumeration type for possible states of a domain.
  */
sealed trait DomainState extends Product with Serializable

object DomainState {

  /**
    * Initial state for all domains.
    */
  final case object Initial extends DomainState

  /**
    * State used for all domains that have been created and later possibly updated.
    *
    * @param id          the unique identifier for the domain
    * @param rev         the selected revision number
    * @param deprecated  the deprecation status
    * @param description the domain description
    */
  final case class Current(
    id: DomainId,
    rev: Long,
    deprecated: Boolean,
    description: String) extends DomainState

}