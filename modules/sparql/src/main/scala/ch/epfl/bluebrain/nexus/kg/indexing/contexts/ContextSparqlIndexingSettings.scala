package ch.epfl.bluebrain.nexus.kg.indexing.contexts

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to context indexing in the triple store.
  *
  * @param contextsBase   the application base uri for operating on contexts
  * @param contextsBaseNs the context base context
  * @param nexusVocBase   the nexus core vocabulary base
  */
final case class ContextSparqlIndexingSettings(contextsBase: Uri, contextsBaseNs: Uri, nexusVocBase: Uri)
