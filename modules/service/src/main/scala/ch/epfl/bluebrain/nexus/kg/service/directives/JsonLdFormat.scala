package ch.epfl.bluebrain.nexus.kg.service.directives

import org.apache.jena.riot.RDFFormat

/**
  * Enumeration type for supported JSON-LD output formats.
  */
sealed trait JsonLdFormat

/**
  * JSON-LD output formats supported by Jena
  *
  * @param toJena the underlying Jena representation.
  */
sealed abstract class JenaJsonLdFormat(val toJena: RDFFormat) extends JsonLdFormat

object JsonLdFormat {

  final case object Default extends JsonLdFormat

  final case object Compacted extends JenaJsonLdFormat(RDFFormat.JSONLD_COMPACT_FLAT)

  final case object Expanded extends JenaJsonLdFormat(RDFFormat.JSONLD_EXPAND_FLAT)

  final case object Flattened extends JenaJsonLdFormat(RDFFormat.JSONLD_FLATTEN_FLAT)

  final case object Framed extends JenaJsonLdFormat(RDFFormat.JSONLD_FRAME_FLAT)
}
