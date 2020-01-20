package ch.epfl.bluebrain.nexus.kg.client

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._

import scala.concurrent.duration._

/**
  * Configuration for [[KgClient]]
  *
  * @param publicIri     base URL for KG service
  * @param sseRetryDelay delay for retrying after completion on SSE. 1 second by default
  */
final case class KgClientConfig(publicIri: AbsoluteIri, prefix: String, sseRetryDelay: FiniteDuration = 1.second) {
  lazy val resourcesIri = publicIri + (prefix / "resources")
}
