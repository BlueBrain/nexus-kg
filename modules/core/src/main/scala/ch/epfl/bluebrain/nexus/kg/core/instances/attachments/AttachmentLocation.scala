package ch.epfl.bluebrain.nexus.kg.core.instances.attachments

import java.net.URI
import java.nio.file.Path
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation.Location

/**
  * Provides the signature for the methods that deals with attachment's location.
  *
  * @tparam F the monadic effect type
  */
trait AttachmentLocation[F[_]] {

  /**
    * Constructs an absolute URI from a relative route.
    *
    * @param relative the relative route to an attachment
    * @return the absolute URI identifying an attachment's location
    */
  def toAbsoluteURI(relative: String): URI

  /**
    * Attempts to create a location for an attachment.
    *
    * @param id  the unique identifier of the instance
    * @param rev the instance revision number
    * @return ''Location'' or the appropriate Fault in the ''F'' context
    */
  def apply(id: InstanceId, rev: Long): F[Location]
}

object AttachmentLocation {

  /**
    * Wraps both the absolute and relative information about the attachment's location
    *
    * @param path     absolute Path where to find the attachment
    * @param relative relative route to the attachment's location
    */
  final case class Location(path: Path, relative: String)

}
