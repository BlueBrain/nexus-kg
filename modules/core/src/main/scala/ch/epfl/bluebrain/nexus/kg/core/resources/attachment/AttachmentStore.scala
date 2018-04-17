package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import cats.MonadError
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.kg.core.resources.RepresentationId
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.SourceWrapper

/**
  * A store for attachments. It delegates the location resolution responsibility and the
  * stream sink and source functionality to the provided ''locator'' and ''fileStream''
  *
  * @param locator    the underlying handling of location resolver for attachments
  * @param fileStream the underlying handling of incoming and outgoing streams
  * @param F          a MonadError typeclass instance for ''F[_]''
  * @tparam F   the monadic effect type
  * @tparam In  a type defining the incoming stream client -> service
  * @tparam Out a type defining the outgoing stream service -> client
  */
class AttachmentStore[F[_], In, Out](locator: LocationResolver[F], fileStream: FileStream.Aux[F, In, Out])(
    implicit F: MonadError[F, Throwable]) {

  /**
    * Stores the provided stream source delegating to ''locator'' for choosing the location
    * and to ''fileStream'' for storing the source on the selected location.
    *
    * @param id            the unique representation identifier of the resource's attachment
    * @param rev           the revision of the resource where the attachment is added
    * @param wrappedSource the source with its metadata
    * @return an [[Attachment]] wrapped in the abstract ''F[_]''  type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.core.rejections.Fault]] wrapped within ''F[_]'' otherwise
    */
  def save(id: RepresentationId, rev: Long, wrappedSource: SourceWrapper[In]): F[Attachment] =
    locator(id, rev, wrappedSource.filename) flatMap (location => fileStream.toSink(location, wrappedSource))

  /**
    * Fetches the binary associated to the provided ''attachment'' delegating to ''locator'' for choosing the location
    * where to retrieve it and to ''fileStream'' for retrieving it.
    *
    * @param attachment the attachment metadata
    */
  def fetch(attachment: Attachment): F[Out] =
    fileStream.toSource(locator.absoluteUri(attachment.fileUri))

}

object AttachmentStore {
  final def apply[F[_], In, Out](locator: LocationResolver[F], fileStream: FileStream.Aux[F, In, Out])(
      implicit F: MonadError[F, Throwable]): AttachmentStore[F, In, Out] =
    new AttachmentStore[F, In, Out](locator, fileStream)
}
