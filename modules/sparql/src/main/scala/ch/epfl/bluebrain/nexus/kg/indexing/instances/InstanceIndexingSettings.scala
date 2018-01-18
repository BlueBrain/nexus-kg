package ch.epfl.bluebrain.nexus.kg.indexing.instances

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to instance indexing.
  *
  * @param index          the name of the index
  * @param instanceBase   the application base uri for operating on instances
  * @param instanceBaseNs the instance base context
  * @param nexusVocBase   the nexus core vocabulary base
  */
final case class InstanceIndexingSettings(index: String, instanceBase: Uri, instanceBaseNs: Uri, nexusVocBase: Uri)