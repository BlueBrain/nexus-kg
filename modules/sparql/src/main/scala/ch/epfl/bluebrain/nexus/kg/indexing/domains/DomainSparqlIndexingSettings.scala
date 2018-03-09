package ch.epfl.bluebrain.nexus.kg.indexing.domains

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to domain indexing in the triple store.
  *
  * @param index        the name of the index
  * @param domainBase   the application base uri for operating on domains
  * @param domainBaseNs the domain base context
  * @param nexusVocBase the nexus core vocabulary base
  */
final case class DomainSparqlIndexingSettings(index: String, domainBase: Uri, domainBaseNs: Uri, nexusVocBase: Uri)
