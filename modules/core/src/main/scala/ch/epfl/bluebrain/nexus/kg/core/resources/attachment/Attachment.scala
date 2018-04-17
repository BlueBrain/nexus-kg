package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.Info

/**
  * Holds all the metadata information related to an attachment.
  *
  * @param fileUri uri where the attachment gets stored. TODO: Change the type to a refined type
  * @param info    extra information about the attachment
  */
final case class Attachment(fileUri: String, info: Info) {
  val name: String = info.filename
}

object Attachment {

// TODO: Use refined type for fileUri
//    final def apply(uri: String, info: Info): Either[String, Attachment] =
//      if (uri == null || uri.isEmpty) Left("'fileUri' field cannot be empty")
//      else Right(new Attachment(uri, info))

  /**
    * Holds all metadata information related to an attachment
    * that we want to expose through the service.
    *
    * @param filename the original filename of the attached file
    * @param mediaType        the media type of the attached file
    * @param contentSize      the size of the attached file
    * @param digest           the digest information of the attached file
    */
  final case class Info(filename: String, mediaType: String, contentSize: Size, digest: Digest)

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
    * The source wrapped with its metadata
    *
    * @param filename  the filename of the source
    * @param mediaType the media type of the source asserted by the client
    * @param source    the source of data
    * @tparam In the source type
    */
  final case class SourceWrapper[In](filename: String, mediaType: String, source: In)
}
