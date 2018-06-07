package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * The unique id of a resource.
  *
  * @param parent the parent reference of the resource identified by this id
  * @param value  he unique identifier for the resource within the referenced parent
  */
final case class Id[P](parent: P, value: AbsoluteIri)
