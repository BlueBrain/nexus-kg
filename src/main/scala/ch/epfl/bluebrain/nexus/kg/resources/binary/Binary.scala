package ch.epfl.bluebrain.nexus.kg.resources.binary

import java.nio.file.Path
import java.util.UUID

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

object Binary {

  /**
    * Holds some of the metadata information related to a binary.
    *
    * @param uuid      the unique id that identifies this binary.
    * @param filename  the original filename of the binary
    * @param mediaType the media type of the binary
    */
  final case class BinaryDescription(uuid: String, filename: String, mediaType: String) {
    def process(stored: StoredSummary): BinaryAttributes =
      BinaryAttributes(uuid, stored.filePath, filename, mediaType, stored.bytes, stored.digest)
  }

  object BinaryDescription {
    def apply(filename: String, mediaType: String): BinaryDescription =
      BinaryDescription(randomUUID(), filename, mediaType)
  }

  /**
    * Holds all the metadata information related to the binary.
    *
    * @param uuid      the unique id that identifies this binary.
    * @param filePath  path where the binary gets stored.
    * @param filename  the original filename of the binary
    * @param mediaType the media type of the binary
    * @param byteSize  the size of the binary file in bytes
    * @param digest    the digest information of the binary
    */
  final case class BinaryAttributes(uuid: String,
                                    filePath: Path,
                                    filename: String,
                                    mediaType: String,
                                    byteSize: Long,
                                    digest: Digest)
  object BinaryAttributes {
    def apply(filePath: Path, filename: String, mediaType: String, size: Long, digest: Digest): BinaryAttributes =
      BinaryAttributes(randomUUID(), filePath, filename, mediaType, size, digest)
  }

  private def randomUUID(): String = UUID.randomUUID().toString.toLowerCase()

  /**
    * Digest related information of the binary
    *
    * @param algorithm the algorithm used in order to compute the digest
    * @param value     the actual value of the digest of the binary
    */
  final case class Digest(algorithm: String, value: String)

  /**
    * The summary after the file has been stored
    *
    * @param filePath the location where the file has been stored
    * @param bytes    the size of the binary in bytes
    * @param digest   the digest related information of the binary
    */
  final case class StoredSummary(filePath: Path, bytes: Long, digest: Digest)

  implicit val digestDecoder: Decoder[Digest] = deriveDecoder[Digest]
}
