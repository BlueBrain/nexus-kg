package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.FileStream.StoredSummary

object Attachment {

  /**
    * Holds some of the metadata information related to an attachment.
    *
    * @param uuid      the unique id that identifies this attachment. TODO: Change the type to a refined type
    * @param filename  the original filename of the attached file
    * @param mediaType the media type of the attached file
    */
  final case class BinaryDescription(uuid: String, filename: String, mediaType: String) {
    def process(stored: StoredSummary): BinaryAttributes =
      BinaryAttributes(uuid, stored.fileUri, filename, mediaType, stored.size, stored.digest)
  }

  object BinaryDescription {
    def apply(filename: String, mediaType: String): BinaryDescription =
      BinaryDescription(UUID.randomUUID().toString.toLowerCase(), filename, mediaType)
  }

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param uuid        the unique id that identifies this attachment. TODO: Change the type to a refined type
    * @param fileUri     uri where the attachment gets stored. TODO: Change the type to a refined type
    * @param filename    the original filename of the attached file
    * @param mediaType   the media type of the attached file
    * @param contentSize the size of the attached file
    * @param digest      the digest information of the attached file
    */
  final case class BinaryAttributes(uuid: String,
                                    fileUri: String,
                                    filename: String,
                                    mediaType: String,
                                    contentSize: Size,
                                    digest: Digest)
  object BinaryAttributes {
    def apply(fileUri: String, filename: String, mediaType: String, size: Size, digest: Digest): BinaryAttributes =
      BinaryAttributes(UUID.randomUUID().toString.toLowerCase(), fileUri, filename, mediaType, size, digest)
  }

// TODO: Use refined type for fileUri
//    final def apply(uri: String, info: Info): Either[String, Attachment] =
//      if (uri == null || uri.isEmpty) Left("'fileUri' field cannot be empty")
//      else Right(new Attachment(uri, info))

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
}
