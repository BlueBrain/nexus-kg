package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.FileStream.StoredSummary
import eu.timepit.refined.api.RefType.refinedRefType.unsafeWrap
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.string.Uuid
object Attachment {

  type UUID = String Refined Uuid

  //TODO: Replace with the model defined in RDF Graph
  type RelativeUri = String Refined NonEmpty

  /**
    * Holds some of the metadata information related to an attachment.
    *
    * @param uuid      the unique id that identifies this attachment.
    * @param filename  the original filename of the attached file
    * @param mediaType the media type of the attached file
    */
  final case class BinaryDescription(uuid: UUID, filename: String, mediaType: String) {
    def process(stored: StoredSummary): BinaryAttributes =
      BinaryAttributes(uuid, stored.fileUri, filename, mediaType, stored.size, stored.digest)
  }

  object BinaryDescription {
    def apply(filename: String, mediaType: String): BinaryDescription =
      BinaryDescription(randomUUID(), filename, mediaType)
  }

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param uuid        the unique id that identifies this attachment.
    * @param fileUri     uri where the attachment gets stored.
    * @param filename    the original filename of the attached file
    * @param mediaType   the media type of the attached file
    * @param contentSize the size of the attached file
    * @param digest      the digest information of the attached file
    */
  final case class BinaryAttributes(uuid: UUID,
                                    fileUri: RelativeUri,
                                    filename: String,
                                    mediaType: String,
                                    contentSize: Size,
                                    digest: Digest)
  object BinaryAttributes {
    def apply(fileUri: RelativeUri, filename: String, mediaType: String, size: Size, digest: Digest): BinaryAttributes =
      BinaryAttributes(randomUUID(), fileUri, filename, mediaType, size, digest)
  }

  private def randomUUID(): UUID = unsafeWrap(UUID.randomUUID().toString.toLowerCase())

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
