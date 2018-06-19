package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Instance attachments specific settings.
  * This should be placed inside the AppConfig case class where the app.conf file gets mapped
  *
  * @param volume          the base Iri where the attachments are stored
  * @param digestAlgorithm the algorithm used to compute the digest of the binary
  */
final case class AttachmentConfig(volume: AbsoluteIri, digestAlgorithm: String)
