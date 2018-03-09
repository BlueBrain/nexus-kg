package ch.epfl.bluebrain.nexus.kg.core.queries.filtering

import akka.http.scaladsl.model.Uri
import io.circe.Json

/**
  * Filtering specific settings.
  *
  * @param nexusBaseVoc   the nexus base vocabulary uri
  * @param ctx            the filtering JSON-LD context value
  */
final case class FilteringSettings(nexusBaseVoc: Uri, ctx: Json)
