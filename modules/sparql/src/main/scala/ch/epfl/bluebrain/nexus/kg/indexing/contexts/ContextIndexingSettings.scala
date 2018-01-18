package ch.epfl.bluebrain.nexus.kg.indexing.contexts

import akka.http.scaladsl.model.Uri

final case class ContextIndexingSettings(index: String, contextsBase: Uri, contextsBaseNs: Uri, nexusVocBase: Uri)
