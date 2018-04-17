package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{Info, Processed}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.FileStream.StoredSummary

/**
  * Holds all the metadata information related to an attachment.
  */
sealed trait Attachment extends Product with Serializable {

  def uuid: String

  def info: Info

  lazy val name: String = info.filename

  def process(stored: StoredSummary): Processed =
    Processed(uuid, stored.fileUri, Info.Total(info.filename, info.mediaType, stored.size, stored.digest))
}

object Attachment {

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param uuid the unique id that identifies this attachment. TODO: Change the type to a refined type
    * @param info    extra information about the attachment
    */
  final case class Unprocessed(uuid: String, info: Info.Partial) extends Attachment

  object Unprocessed {
    def apply(info: Info.Partial): Unprocessed = Unprocessed(UUID.randomUUID().toString.toLowerCase(), info)
  }

  /**
    * Holds all the metadata information related to an attachment.
    *
    * @param uuid the unique id that identifies this attachment. TODO: Change the type to a refined type
    * @param fileUri uri where the attachment gets stored. TODO: Change the type to a refined type
    * @param info    extra information about the attachment
    */
  final case class Processed(uuid: String, fileUri: String, info: Info.Total) extends Attachment
  object Processed {
    def apply(fileUri: String, info: Info.Total): Processed =
      Processed(UUID.randomUUID().toString.toLowerCase(), fileUri, info)
  }

// TODO: Use refined type for fileUri
//    final def apply(uri: String, info: Info): Either[String, Attachment] =
//      if (uri == null || uri.isEmpty) Left("'fileUri' field cannot be empty")
//      else Right(new Attachment(uri, info))

  sealed trait Info extends Product with Serializable {
    def filename: String
    def mediaType: String
  }

  object Info {

    /**
      * Holds part of the metadata information related to an attachment.
      *
      * @param filename the original filename of the attached file
      * @param mediaType        the media type of the attached file
      */
    final case class Partial(filename: String, mediaType: String) extends Info

    /**
      * Holds all metadata information related to an attachment
      * that we want to expose through the service.
      *
      * @param filename the original filename of the attached file
      * @param mediaType        the media type of the attached file
      * @param contentSize      the size of the attached file
      * @param digest           the digest information of the attached file
      */
    final case class Total(filename: String, mediaType: String, contentSize: Size, digest: Digest) extends Info
  }

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
