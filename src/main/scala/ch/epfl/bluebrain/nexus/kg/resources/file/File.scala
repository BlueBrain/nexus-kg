package ch.epfl.bluebrain.nexus.kg.resources.file

import java.util.UUID

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.config.Contexts.storageCtx
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidJsonLD, InvalidResourceFormat}
import ch.epfl.bluebrain.nexus.kg.resources.{Rejection, ResId}
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder._
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder.stringEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json

import scala.util.Try

object File {

  val write: Permission = Permission.unsafe("files/write")

  /**
    * Holds the metadata information related to a file link.
    *
    * @param location  the target file location
    * @param filename  the original filename
    * @param mediaType the media type
    */
  final case class LinkDescription(location: Uri, filename: String, mediaType: String)

  object LinkDescription {

    private implicit val uriEncoder: NodeEncoder[Uri] = node =>
      stringEncoder(node).flatMap { uri =>
        Try(Uri(uri)).toEither.left.map(err => IllegalConversion(s"Invalid URI '${err.getMessage}'"))
    }

    /**
      * Attempts to transform a JSON payload into a [[LinkDescription]].
      *
      * @param id     the resource identifier
      * @param source the JSON payload
      * @return a link description if the resource is compatible or a rejection otherwise
      */
    final def apply(id: ResId, source: Json): Either[Rejection, LinkDescription] =
      for {
        graph <- source
          .replaceContext(storageCtx)
          .id(id.value)
          .asGraph(id.value)
          .left
          .map(_ => InvalidJsonLD("Invalid JSON payload."))
        c = graph.cursor()
        filename <- c
          .downField(nxv.filename)
          .focus
          .as[String]
          .left
          .map(_ => InvalidResourceFormat(id.ref, "Invalid 'filename' field."))
        mediaType <- c
          .downField(nxv.mediaType)
          .focus
          .as[String]
          .left
          .map(_ => InvalidResourceFormat(id.ref, "Invalid 'mediaType' field."))
        location <- c
          .downField(nxv.location)
          .focus
          .as[Uri]
          .left
          .map(_ => InvalidResourceFormat(id.ref, "Invalid 'location' field."))
      } yield LinkDescription(location, filename, mediaType)

  }

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
