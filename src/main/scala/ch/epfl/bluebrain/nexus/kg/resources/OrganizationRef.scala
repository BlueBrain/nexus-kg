package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A stable organization reference.
  *
  * @param id The unique identifier for the organization
  */
final case class OrganizationRef(id: AbsoluteIri)


