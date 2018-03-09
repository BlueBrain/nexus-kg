package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to organization indexing in the triple store.
  *
  * @param index        the name of the index
  * @param orgBase      the application base uri for operating on organization
  * @param orgBaseNs    the organization base context
  * @param nexusVocBase the nexus core vocabulary base
  */
final case class OrganizationSparqlIndexingSettings(index: String, orgBase: Uri, orgBaseNs: Uri, nexusVocBase: Uri)
