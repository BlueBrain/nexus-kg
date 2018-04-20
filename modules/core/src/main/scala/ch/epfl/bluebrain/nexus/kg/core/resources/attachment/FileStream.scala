package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.net.URI

import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{Digest, RelativeUri, Size}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.FileStream.StoredSummary
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.LocationResolver.Location

/**
  * Defines the signature for methods which deal with incoming and outgoing streams.
  *
  * @tparam F   the monadic effect type
  */
trait FileStream[F[_]] {

  /**
    * a type defining the incoming stream client -> service
    */
  type In

  /**
    * a type defining the outgoing stream service -> client
    */
  type Out

  /**
    * Attempts to create a Out form a URI.
    * This should be used to transmit the content referred by the URI through the Out type in streaming fashion.
    *
    * @param uri the URI from where to retrieve the content
    * @return the typeclass Out
    */
  def toSource(uri: URI): F[Out]

  /**
    * Attempts to store and create metadata information which will be used by [[ch.epfl.bluebrain.nexus.kg.core.resources.State]]
    * from an incoming source of type In (which is typically a stream).
    *
    * @param loc    the location of the attachment
    * @param source the source
    */
  def toSink(loc: Location, source: In): F[StoredSummary]
}

object FileStream {

  /**
    * The summary after the file has been stored
    *
    * @param fileUri the location where the file has been stored
    * @param size    the size of the attached file
    * @param digest  the digest related information of the attached file
    */
  final case class StoredSummary(fileUri: RelativeUri, size: Size, digest: Digest)

  type Aux[F[_], In0, Out0] = FileStream[F] {
    type In  = In0
    type Out = Out0
  }
}
