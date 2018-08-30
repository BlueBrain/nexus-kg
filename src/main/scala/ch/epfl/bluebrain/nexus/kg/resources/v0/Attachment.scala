package ch.epfl.bluebrain.nexus.kg.resources.v0

import java.nio.file.Paths

import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

object Attachment {

  final case class Meta(fileUri: String, info: Info) {
    require(fileUri != null && !fileUri.isEmpty)

    /**
      * @note Parsing content size might fail silently and return -1.
      * @return this instance converted to the new v1 attributes format
      *         [[ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes]]
      */
    def toBinaryAttributes =
      BinaryAttributes(Paths.get(fileUri),
                       info.originalFileName,
                       info.mediaType,
                       convert(info.contentSize),
                       info.digest)
  }

  final case class Info(originalFileName: String, mediaType: String, contentSize: Size, digest: Digest)

  final case class Size(unit: String = "byte", value: Long)

  private def convert(size: Size): Long = size.unit.toLowerCase match {
    case "byte" | "bytes" | "b"          => size.value
    case "kilobyte" | "kilobytes" | "kb" => size.value * 1000L
    case "megabyte" | "megabytes" | "mb" => size.value * 1000000L
    case "gigabyte" | "gigabytes" | "gb" => size.value * 1000000000L
    case "terabyte" | "terabytes" | "tb" => size.value * 1000000000000L
    case "petabyte" | "petabytes" | "pb" => size.value * 1000000000000000L
    case "kib"                           => size.value * 1024L
    case "mib"                           => size.value * 1048576L
    case "gib"                           => size.value * 1073741824L
    case "tib"                           => size.value * 1099511627776L
    case "pib"                           => size.value * 1125899906842624L
    case _                               => -1L
  }

  implicit val sizeDecoder: Decoder[Size]    = deriveDecoder[Size]
  implicit val infoDecoder: Decoder[Info]    = deriveDecoder[Info]
  implicit val attMetaDecoder: Decoder[Meta] = deriveDecoder[Meta]
}
