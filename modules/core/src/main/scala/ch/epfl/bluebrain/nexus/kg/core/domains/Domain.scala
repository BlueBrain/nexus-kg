package ch.epfl.bluebrain.nexus.kg.core.domains

/**
  * Data type representing the state of a domain.
  *
  * @param id          a unique identifier for the domain
  * @param rev         the selected revision for the domain
  * @param deprecated  the deprecation status of the domain
  * @param description a description for the domain
  */
final case class Domain(id: DomainId, rev: Long, deprecated: Boolean, description: String)