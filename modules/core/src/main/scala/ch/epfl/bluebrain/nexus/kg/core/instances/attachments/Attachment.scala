package ch.epfl.bluebrain.nexus.kg.core.instances.attachments

object Attachment {

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param fileUri uri where the attachment gets stored
    * @param info    extra information about the attachment
    */
  final case class Meta(fileUri: String, info: Info) {
    require(fileUri != null && !fileUri.isEmpty)
  }

  final val EmptyMeta = Meta(" ", Info("", "", Size(value = 0L), Digest("", "")))

  /**
    * Holds all metadata information related to an attachment
    * that we want to expose through the service.
    *
    * @param originalFileName the original filename of the attached file
    * @param contentType      the content type of the attached file
    * @param size             the size of the attached file
    * @param digest           the digest information of the attached file
    */
  final case class Info(originalFileName: String, contentType: String, size: Size, digest: Digest)

  /**
    * Digest related information of the attached file
    *
    * @param alg   the algorithm used in order to compute the digest
    * @param value the actual value of the digest of the attached file
    */
  final case class Digest(alg: String, value: String)

  /**
    * The size of the attached file
    *
    * @param unit  the size's unit of the attached file
    * @param value the actual value of the size of the attached file
    */
  final case class Size(unit: String = "byte", value: Long)
}
