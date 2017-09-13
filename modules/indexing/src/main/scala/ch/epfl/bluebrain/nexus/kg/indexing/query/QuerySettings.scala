package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination

/**
  * Collection of configurable settings specific to queries.
  *
  * @param pagination   the default pagination parameters
  * @param index        the index to be used for querying
  * @param nexusVocBase the nexus core vocabulary base
  */
final case class QuerySettings(pagination: Pagination, index: String, nexusVocBase: Uri)