package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{BinaryAttributes, BinaryDescription}

/**
  * A store for attachments. It delegates the location resolution responsibility and the
  * stream sink and source functionality to the provided ''locator'' and ''fileStream''
  *
  * @param loc        the underlying handling of location resolver for attachments
  * @param fileStream the underlying handling of incoming and outgoing streams
  * @param F          a MonadError typeclass instance for ''F[_]''
  * @tparam F   the monadic effect type
  * @tparam In  a type defining the incoming stream client -> service
  * @tparam Out a type defining the outgoing stream service -> client
  */
class AttachmentStore[F[_], In, Out](loc: LocationResolver[F], fileStream: FileStream.Aux[F, In, Out])(
    implicit F: MonadError[F, Throwable]) {

  /**
    * Stores the provided stream source delegating to ''locator'' for choosing the location
    * and to ''fileStream'' for storing the source on the selected location.
    *
    * @param projectReference the project reference for this attachment
    * @param att              the attachment to be stored
    * @param source           the source
    * @return [[BinaryAttributes]] wrapped in the abstract ''F[_]''  type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def save(projectReference: String, att: BinaryDescription, source: In): F[BinaryAttributes] =
    loc(projectReference, att) flatMap (location => fileStream.toSink(location, source)) map (att.process(_))

  /**
    * Fetches the binary associated to the provided ''attachment'' delegating to ''locator'' for choosing the location
    * where to retrieve it and to ''fileStream'' for retrieving it.
    *
    * @param attachment the attachment metadata
    */
  def fetch(attachment: BinaryAttributes): F[Out] =
    fileStream.toSource(loc.absoluteUri(attachment.fileUri))

}

object AttachmentStore {
  final def apply[F[_], In, Out](locator: LocationResolver[F], fileStream: FileStream.Aux[F, In, Out])(
      implicit F: MonadError[F, Throwable]): AttachmentStore[F, In, Out] =
    new AttachmentStore[F, In, Out](locator, fileStream)
}
