package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to schema indexing in the triple store.
  *
  * @param schemasBase   the application base uri for operating on schemas
  * @param schemasBaseNs the schema base context
  * @param nexusVocBase  the nexus core vocabulary base
  */
final case class SchemaSparqlIndexingSettings(schemasBase: Uri, schemasBaseNs: Uri, nexusVocBase: Uri)