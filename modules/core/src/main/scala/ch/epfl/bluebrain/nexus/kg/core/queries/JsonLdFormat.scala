package ch.epfl.bluebrain.nexus.kg.core.queries

import io.circe.{Decoder, Encoder}
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

  def fromString(value: String): Option[JsonLdFormat] = value match {
    case "compacted" => Some(Compacted)
    case "expanded"  => Some(Expanded)
    case "flattened" => Some(Flattened)
    case "default"   => Some(Default)
    case _           => None
  }

  implicit final val formatDecoder: Decoder[JsonLdFormat] =
    Decoder.decodeString.emap(v => fromString(v).toRight(s"Could not find JsonLdFormat for '$v'"))

  implicit final val formatEncoder: Encoder[JsonLdFormat] =
    Encoder.encodeString.contramap(_.toString.toLowerCase())

}
