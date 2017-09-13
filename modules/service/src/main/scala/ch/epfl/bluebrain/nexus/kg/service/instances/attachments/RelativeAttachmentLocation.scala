package ch.epfl.bluebrain.nexus.kg.service.instances.attachments

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.core.Fault.Unexpected
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation.Location
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import journal.Logger

import scala.util.Try

/**
  * Partially implements AttachmentLocation and
  * adds a base path constructor
  *
  * @param base path of the root directory from where to store attachments
  * @tparam F the monadic effect type
  */
abstract class RelativeAttachmentLocation[F[_]](base: Path)
  extends AttachmentLocation[F] {

  override def toAbsoluteURI(relative: String): URI = new File(base.toFile, relative).toURI
}

object RelativeAttachmentLocation {

  /**
    * Constructs a ''RelativeAttachmentLocation'' from base path implementing
    * the missing apply method.
    *
    * Generates a relative path as follows:
    * given an id = 017f9837-5bea-4e79-bdbd-e64246cd81ec
    * and a rev = 3
    * creates a relative route = 0/1/7/f/9/8/3/7/017f9837-5bea-4e79-bdbd-e64246cd81ec.4
    *
    * @param base path of the root directory from where to store attachments
    * @param F    a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    * @return the RelativeAttachmentLocation
    */
  def apply[F[_]](base: Path)(implicit F: MonadError[F, Throwable]) = new RelativeAttachmentLocation[F](base) {

    private val logger = Logger[this.type]

    override def apply(id: InstanceId, rev: Long): F[Location] = {

      def getAttachmentRelativePath: String =
        s"${id.id.takeWhile(_ != '-').mkString("/")}/${id.id}.$rev"

      Try {
        val relative = getAttachmentRelativePath
        val attachmentPath = new File(base.toFile, relative).toPath
        Files.createDirectories(attachmentPath.getParent)
        Location(attachmentPath, relative)
      }.fold(
        error => {
          logger.error(s"Error while trying to create the directory for instance '$id'", error)
          F.raiseError(Unexpected(s"I/O error while trying to create directory for instance '$id'. Error '${error.getMessage}'"))
        },
        location => F.pure(location)
      )
    }
  }

  /**
    * Constructs a ''RelativeAttachmentLocation'' from settings
    *
    * @param settings application configuration
    * @param F        a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    * @return the RelativeAttachmentLocation
    */
  def apply[F[_]](settings: Settings)(implicit F: MonadError[F, Throwable]): RelativeAttachmentLocation[F] =
    apply[F](settings.Attachment.VolumePath)

}




