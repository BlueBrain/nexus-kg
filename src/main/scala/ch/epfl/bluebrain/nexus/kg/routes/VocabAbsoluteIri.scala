package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * AbsoluteIri that gets expanded using the @vocab instead of the @base
  * on the default case
  *
  * @param value the absolute iri
  */
private[routes] final case class VocabAbsoluteIri(value: AbsoluteIri)
