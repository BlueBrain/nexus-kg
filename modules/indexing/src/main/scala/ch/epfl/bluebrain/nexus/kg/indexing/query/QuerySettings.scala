package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination

/**
  * Collection of configurable settings specific to queries.
  *
  * @param pagination   the default pagination parameters
  * @param maxSize      the maximum size parameter
  * @param index        the index to be used for querying
  * @param nexusVocBase the nexus core vocabulary base
  * @param base         the service public uri + prefix
  * @param aclGraph     the graph where ACLs are indexed
  */
final case class QuerySettings(pagination: Pagination,
                               maxSize: Int,
                               index: String,
                               nexusVocBase: Uri,
                               base: Uri,
                               aclGraph: Uri)
