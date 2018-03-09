package ch.epfl.bluebrain.nexus.kg.core.instances.attachments

import java.net.URI

import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._

/**
  * Defines the signature for methods which deal with incoming and outgoing streams.
  *
  * @tparam F   the monadic effect type
  * @tparam In  a type defining the incoming stream client -> service
  * @tparam Out a type defining the outgoing stream service -> client
  */
trait InOutFileStream[F[_], In, Out] {

  /**
    * Attempts to create a Out form a URI.
    * This should be used to transmit the content referred by the URI through the Out type in streaming fashion.
    *
    * @param uri the URI from where to retrieve the content
    * @return the typeclass Out
    */
  def toSource(uri: URI): F[Out]

  /**
    * Attempts to store and create metadata information which will be used by [[ch.epfl.bluebrain.nexus.kg.core.instances.InstanceState]]
    * from an incoming source of type In (which is typically a stream).
    *
    * @param id          the unique identifier of the instance
    * @param rev         the instance revision number
    * @param filename    the filename of the source
    * @param contentType the content type of the source asserted by the client
    * @param source      the source of data
    * @return the metadata information of the source
    *         or the appropriate Fault in the ''F'' context
    */
  def toSink(id: InstanceId, rev: Long, filename: String, contentType: String, source: In): F[Meta]
}
