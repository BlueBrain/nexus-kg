package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.Path
import java.util.UUID

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

object File {

  /**
    * Holds some of the metadata information related to a file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    */
  final case class FileDescription(uuid: String, filename: String, mediaType: String) {
    def process(stored: StoredSummary): FileAttributes =
      FileAttributes(uuid, stored.filePath, filename, mediaType, stored.bytes, stored.digest)
  }

  object FileDescription {
    def apply(filename: String, mediaType: String): FileDescription =
      FileDescription(randomUUID, filename, mediaType)
  }

  /**
    * Holds all the metadata information related to the file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param filePath  path where the file gets stored.
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    * @param bytes     the size of the file file in bytes
    * @param digest    the digest information of the file
    */
  final case class FileAttributes(uuid: String,
                                  filePath: Path,
                                  filename: String,
                                  mediaType: String,
                                  bytes: Long,
                                  digest: Digest)
  object FileAttributes {
    def apply(filePath: Path, filename: String, mediaType: String, size: Long, digest: Digest): FileAttributes =
      FileAttributes(randomUUID, filePath, filename, mediaType, size, digest)
  }

  private def randomUUID: String = UUID.randomUUID.toString.toLowerCase

  /**
    * Digest related information of the file
    *
    * @param algorithm the algorithm used in order to compute the digest
    * @param value     the actual value of the digest of the file
    */
  final case class Digest(algorithm: String, value: String)

  /**
    * The summary after the file has been stored
    *
    * @param filePath the location where the file has been stored
    * @param bytes    the size of the file in bytes
    * @param digest   the digest related information of the file
    */
  final case class StoredSummary(filePath: Path, bytes: Long, digest: Digest)

  implicit val digestDecoder: Decoder[Digest] = deriveDecoder[Digest]
}
