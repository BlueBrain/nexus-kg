package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.Show
import cats.syntax.show._
import io.circe.{Decoder, Encoder}

import scala.util.Try

/**
  * A stable organization reference.
  *
  * @param id the underlying stable identifier for an organization
  */
final case class OrganizationRef(id: UUID)

object OrganizationRef {

  final implicit val orgRefShow: Show[OrganizationRef] = Show.show(_.id.toString)
  final implicit val orgRefEncoder: Encoder[OrganizationRef] =
    Encoder.encodeString.contramap(_.show)
  final implicit val orgRefDecoder: Decoder[OrganizationRef] =
    Decoder.decodeString.emapTry(uuid => Try(UUID.fromString(uuid)).map(OrganizationRef.apply))
}
