package ch.epfl.bluebrain.nexus.kg.resources.attachment

import java.nio.file.Path
import java.util.UUID

object Attachment {

  /**
    * Holds some of the metadata information related to an attachment.
    *
    * @param uuid      the unique id that identifies this attachment.
    * @param filename  the original filename of the attached file
    * @param mediaType the media type of the attached file
    */
  final case class BinaryDescription(uuid: String, filename: String, mediaType: String) {
    def process(stored: StoredSummary): BinaryAttributes =
      BinaryAttributes(uuid, stored.filePath, filename, mediaType, stored.size, stored.digest)
  }

  object BinaryDescription {
    def apply(filename: String, mediaType: String): BinaryDescription =
      BinaryDescription(randomUUID(), filename, mediaType)
  }

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param uuid        the unique id that identifies this attachment.
    * @param filePath    path where the attachment gets stored.
    * @param filename    the original filename of the attached file
    * @param mediaType   the media type of the attached file
    * @param contentSize the size of the attached file
    * @param digest      the digest information of the attached file
    */
  final case class BinaryAttributes(uuid: String,
                                    filePath: Path,
                                    filename: String,
                                    mediaType: String,
                                    contentSize: Size,
                                    digest: Digest)
  object BinaryAttributes {
    def apply(filePath: Path, filename: String, mediaType: String, size: Size, digest: Digest): BinaryAttributes =
      BinaryAttributes(randomUUID(), filePath, filename, mediaType, size, digest)
  }

  private def randomUUID(): String = UUID.randomUUID().toString.toLowerCase()

  /**
    * Digest related information of the attached file
    *
    * @param algorithm the algorithm used in order to compute the digest
    * @param value     the actual value of the digest of the attached file
    */
  final case class Digest(algorithm: String, value: String)

  /**
    * The size of the attached file
    *
    * @param unit  the size's unit of the attached file
    * @param value the actual value of the size of the attached file
    */
  final case class Size(unit: String = "byte", value: Long)

  /**
    * The summary after the file has been stored
    *
    * @param filePath the location where the file has been stored
    * @param size     the size of the attached file
    * @param digest   the digest related information of the attached file
    */
  final case class StoredSummary(filePath: Path, size: Size, digest: Digest)
}
