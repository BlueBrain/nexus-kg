package ch.epfl.bluebrain.nexus.kg.resources.v0

import java.time.Instant

import ch.epfl.bluebrain.nexus.commons.types.identity.Identity
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types.{Identity => IamIdentity}
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveDecoder
import io.circe.java8.time._

/**
  * v0 Meta
  *
  * @param author
  * @param instant
  */
final case class Meta(author: Identity, instant: Instant) {
  /**
    * @return the author converted to the new [[ch.epfl.bluebrain.nexus.iam.client.types.Identity]] format
    */
  def toIdentity: IamIdentity = author match {
    case _: Anonymous        => IamIdentity.Anonymous
    case a: AuthenticatedRef => IamIdentity.AuthenticatedRef(a.realm)
    case u: UserRef          => IamIdentity.UserRef(u.realm, u.sub)
    case g: GroupRef         => IamIdentity.GroupRef(g.realm, g.group)
  }
}

object Meta {
  implicit val configuration: Configuration = Configuration.default.withDiscriminator("type")
  implicit val metaDecoder: Decoder[Meta] = deriveDecoder[Meta]
}
