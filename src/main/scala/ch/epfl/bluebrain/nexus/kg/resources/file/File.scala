package ch.epfl.bluebrain.nexus.kg.resources.file

import java.security.MessageDigest
import java.util.UUID

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Path.{Empty, Segment, SingleSlash}
import akka.http.scaladsl.model.{ContentType, ContentTypes, Uri}
import ch.epfl.bluebrain.nexus.commons.rdf.instances._
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidJsonLD, InvalidResourceFormat}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{nonEmpty, Rejection, ResId}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder._
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder.{stringEncoder, EncoderResult}
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json

import scala.annotation.tailrec
import scala.util.Try

object File {

  val write: Permission = Permission.unsafe("files/write")

  /**
    * Holds the metadata information related to a file link.
    *
    * @param path      the target file relative location (from the storage)
    * @param filename  an optional filename
    * @param mediaType an optional media type
    */
  final case class LinkDescription(path: Path, filename: Option[String], mediaType: Option[ContentType])

  object LinkDescription {

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
        filename <- c.downField(nxv.filename).focus match {
          case None => Right(None)
          case Some(node) =>
            node
              .as[String]
              .flatMap(name => nonEmpty(name, nxv.filename.prefix))
              .map(Option.apply)
              .toRejectionOnLeft(id.ref)
        }
        mediaType <- c.downField(nxv.mediaType).focus match {
          case None       => Right(None)
          case Some(node) => node.as[ContentType].map(Option.apply).toRejectionOnLeft(id.ref)
        }
        path <- c.downField(nxv.path).focus.as[Path].toRejectionOnLeft(id.ref)
      } yield LinkDescription(path, filename, mediaType)
  }

  /**
    * Holds some of the metadata information related to a file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    */
  final case class FileDescription(uuid: UUID, filename: String, mediaType: ContentType) {
    def process(stored: StoredSummary): FileAttributes =
      FileAttributes(uuid, stored.location, stored.path, filename, mediaType, stored.bytes, stored.digest)
  }

  object FileDescription {
    def apply(filename: String, mediaType: ContentType): FileDescription =
      FileDescription(UUID.randomUUID, filename, mediaType)

    def from(link: LinkDescription): FileDescription =
      FileDescription(
        UUID.randomUUID,
        link.filename.getOrElse(getFilename(link.path)),
        link.mediaType.getOrElse(ContentTypes.`application/octet-stream`)
      )

    @tailrec
    private def getFilename(path: Path): String = path match {
      case Empty | SingleSlash        => "unknown"
      case Segment(head, Empty)       => head
      case Segment(head, SingleSlash) => head
      case _                          => getFilename(path.tail)
    }
  }

  /**
    * Holds all the metadata information related to the file.
    *
    * @param uuid      the unique id that identifies this file.
    * @param location  the absolute location where the file gets stored
    * @param path      the relative path (from the storage) where the file gets stored
    * @param filename  the original filename of the file
    * @param mediaType the media type of the file
    * @param bytes     the size of the file file in bytes
    * @param digest    the digest information of the file
    */
  final case class FileAttributes(
      uuid: UUID,
      location: Uri,
      path: Path,
      filename: String,
      mediaType: ContentType,
      bytes: Long,
      digest: Digest
  )
  object FileAttributes {

    def apply(
        location: Uri,
        path: Path,
        filename: String,
        mediaType: ContentType,
        size: Long,
        digest: Digest
    ): FileAttributes =
      FileAttributes(UUID.randomUUID, location, path, filename, mediaType, size, digest)
  }

  /**
    * Digest related information of the file
    *
    * @param algorithm the algorithm used in order to compute the digest
    * @param value     the actual value of the digest of the file
    */
  final case class Digest(algorithm: String, value: String)
  object Digest {

    /**
      * Attempts to constructs a Digest from the provided json
      *
      * @param resId the resource identifier
      * @param json  the payload
      * @return Right(digest) when successful and Left(rejection) when failed
      */
    final def apply(resId: ResId, json: Json): Either[Rejection, Digest] =
      // format: off
      for {
        graph       <- (json deepMerge digestCtx).id(resId.value).asGraph(resId.value).left.map(_ => InvalidResourceFormat(resId.ref, "Empty or wrong Json-LD. Both 'algorithm' and 'value' fields must be present."))
        cursor       = graph.cursor()
        _           <- cursor.downField(rdf.tpe).focus.as[AbsoluteIri].flatMap(validType).left.map(_ => InvalidResourceFormat(resId.ref, "'@type' field does not have the right format."))
        algorithm   <- cursor.downField(nxv.algorithm).focus.as[String].flatMap(validAlgorithm).left.map(_ => InvalidResourceFormat(resId.ref, "'algorithm' field does not have the right format."))
        value       <- cursor.downField(nxv.value).focus.as[String].flatMap(nonEmpty(_, "value")).left.map(_ => InvalidResourceFormat(resId.ref, "'value' field does not have the right format."))
      } yield Digest(algorithm, value)
    // format: on

    val empty: Digest = Digest("", "")

    private def validType(iri: AbsoluteIri): EncoderResult[AbsoluteIri] =
      if (iri == nxv.UpdateDigest.value) Right(iri) else Left(IllegalConversion(""))

    private def validAlgorithm(s: String): EncoderResult[String] =
      Try(MessageDigest.getInstance(s)).map(_ => s).toOption.toRight[NodeEncoderError](IllegalConversion(""))

  }

  /**
    * The summary after the file has been stored
    *
    * @param location the absolute location where the file has been stored
    * @param path     the relative path (from the storage) where the file gets stored
    * @param bytes    the size of the file in bytes
    * @param digest   the digest related information of the file
    */
  final case class StoredSummary(location: Uri, path: Path, bytes: Long, digest: Digest)

  object StoredSummary {
    val empty: StoredSummary = StoredSummary(Uri.Empty, Path.Empty, 0L, Digest.empty)
  }

}
