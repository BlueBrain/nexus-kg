package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

//TODO: This should be placed inside the AppConfig case class where the app.conf file gets mapped
final case class AttachmentConfig(volume: AbsoluteIri, digestAlgorithm: String)
