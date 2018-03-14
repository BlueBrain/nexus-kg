package ch.epfl.bluebrain.nexus.kg.indexing.instances

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to instance indexing in the triple store.
  *
  * @param instanceBase   the application base uri for operating on instances
  * @param instanceBaseNs the instance base context
  * @param nexusVocBase   the nexus core vocabulary base
  */
final case class InstanceSparqlIndexingSettings(instanceBase: Uri, instanceBaseNs: Uri, nexusVocBase: Uri)
