package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to organization indexing in the ElasticSearch indexer.
  *
  * @param index        the name of the index
  * @param `type`       the name of the `type`
  * @param orgBase      the application base uri for operating on organization
  * @param nexusVocBase the nexus core vocabulary base
  */
final case class OrganizationEsIndexingSettings(index: String, `type` : String, orgBase: Uri, nexusVocBase: Uri)