package ch.epfl.bluebrain.nexus.kg.resources.file

import java.time.Instant
import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

case class S3Storage(ref: ProjectRef,
                     id: AbsoluteIri,
                     uuid: UUID,
                     rev: Long,
                     instant: Instant,
                     deprecated: Boolean,
                     default: Boolean,
                     digestAlgorithm: String)
    extends Storage
