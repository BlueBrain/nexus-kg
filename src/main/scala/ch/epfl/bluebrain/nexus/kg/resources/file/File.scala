package ch.epfl.bluebrain.nexus.kg.resources.file

import java.util.UUID

import akka.http.scaladsl.model.Uri

object File {

  /**
    * Holds some of the metadata information related to a file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    */
  final case class FileDescription(uuid: UUID, filename: String, mediaType: String) {
    def process(stored: StoredSummary): FileAttributes =
      FileAttributes(uuid, stored.location, filename, mediaType, stored.bytes, stored.digest)
  }

  object FileDescription {
    def apply(filename: String, mediaType: String): FileDescription =
      FileDescription(UUID.randomUUID, filename, mediaType)
  }

  /**
    * Holds all the metadata information related to the file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param location  the absolute location where the file gets stored
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    * @param bytes     the size of the file file in bytes
    * @param digest    the digest information of the file
    */
  final case class FileAttributes(uuid: UUID,
                                  location: Uri,
                                  filename: String,
                                  mediaType: String,
                                  bytes: Long,
                                  digest: Digest)
  object FileAttributes {

    def apply(location: Uri, filename: String, mediaType: String, size: Long, digest: Digest): FileAttributes =
      FileAttributes(UUID.randomUUID, location, filename, mediaType, size, digest)
  }

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
    * @param location the absolute location where the file has been stored
    * @param bytes    the size of the file in bytes
    * @param digest   the digest related information of the file
    */
  final case class StoredSummary(location: Uri, bytes: Long, digest: Digest)

}
