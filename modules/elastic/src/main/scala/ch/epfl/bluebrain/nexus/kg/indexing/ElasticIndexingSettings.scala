package ch.epfl.bluebrain.nexus.kg.indexing

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to organization indexing in the ElasticSearch indexer.
  *
  * @param index        the name of the index
  * @param `type`       the name of the `type`
  * @param base         the application base uri for operating on resources
  * @param nexusVocBase the nexus core vocabulary base
  */
final case class ElasticIndexingSettings(index: String, `type`: String, base: Uri, nexusVocBase: Uri)
