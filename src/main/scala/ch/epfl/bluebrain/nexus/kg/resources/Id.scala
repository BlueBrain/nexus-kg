package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * The unique id of a resource.
  *
  * @param project the parent project reference of the resource identified by this id
  * @param value   the unique identifier for the resource within the referenced project
  */
final case class Id(project: ProjectRef, value: AbsoluteIri)
