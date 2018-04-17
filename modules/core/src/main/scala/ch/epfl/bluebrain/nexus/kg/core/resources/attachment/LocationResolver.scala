package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AttachmentConfig
import ch.epfl.bluebrain.nexus.kg.core.rejections.Fault.Unexpected
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.LocationResolver._
import journal.Logger

import scala.util.{Failure, Success, Try}

/**
  * Manages the location of attachments from a provided ''base''.
  *
  * @param base path of the root directory where to store attachments
  *
  * @tparam F the monadic effect type
  */
abstract class LocationResolver[F[_]](base: Path) {

  /**
    * Constructs an absolute URI from a relative route.
    *
    * @param relative the relative route to an attachment
    * @return the absolute URI identifying an attachment's location
    */
  def absoluteUri(relative: String): URI = new File(base.toFile, relative).toURI

  /**
    * Attempts to create a location for an attachment.
    *
    * @param projectReference the project reference for this attachment
    * @param attachment       the attachment to be stored
    * @return ''Location'' or the appropriate Fault in the ''F'' context
    */
  def apply(projectReference: String, attachment: Attachment): F[Location]
}

object LocationResolver {

  /**
    * Wraps both the absolute and relative information about the attachment's location
    *
    * @param path     absolute Path where to find the attachment
    * @param relative relative route to the attachment's location
    */
  final case class Location(path: Path, relative: String)

  private val logger = Logger[this.type]

  /**
    * Constructs a ''RelativeAttachmentLocation'' from base path implementing
    * the missing apply method.
    *
    * Generates a relative path as follows:
    * given a resourceId = http://schema.org/something
    * and a project = project
    * and a rev = 3
    * creates a relative route = project/something/something.3
    *
    * @param base path of the root directory from where to store attachments
    * @param F    a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    * @return the RelativeAttachmentLocation
    */
  def apply[F[_]](base: Path)(implicit F: MonadError[F, Throwable]): LocationResolver[F] =
    new LocationResolver[F](base) {
      override def apply(projectReference: String, attachment: Attachment): F[Location] = {
        val relativePath =
          s"${projectReference}/${attachment.uuid.takeWhile(_ != '-').mkString("/")}/${attachment.uuid}"

        Try {
          val attachmentPath = new File(base.toFile, relativePath).toPath
          Files.createDirectories(attachmentPath.getParent)
          Location(attachmentPath, relativePath)
        } match {
          case Failure(e) =>
            logger.error(s"Error while trying to create the directory for attachment '$attachment'", e)
            F.raiseError(
              Unexpected(s"I/O error while trying to create directory for attachment '$attachment'. '${e.getMessage}'"))
          case Success(location) => F.pure(location)
        }
      }
    }

  def apply[F[_]](implicit config: AttachmentConfig, F: MonadError[F, Throwable]): LocationResolver[F] =
    apply(config.volume)

}
