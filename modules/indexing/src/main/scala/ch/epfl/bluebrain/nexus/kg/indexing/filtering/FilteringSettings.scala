package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import akka.http.scaladsl.model.Uri

/**
  * Filtering specific settings.
  *
  * @param nexusBaseVoc   the nexus base vocabulary uri
  * @param nexusSearchVoc the nexus search vocabulary uri
  */
final case class FilteringSettings(nexusBaseVoc: Uri, nexusSearchVoc: Uri)