package ch.epfl.bluebrain.nexus.kg.core.organizations

import cats.Show
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex

/**
  * A organization identifier.
  *
  * @param id the unique organization identifier
  */
final case class OrgId(id: String)

object OrgId {

  final val regex: Regex = "([a-zA-Z0-9]+)".r

  final implicit val orgIdShow: Show[OrgId] = Show.show { _.id }

  final implicit val orgIdEncoder: Encoder[OrgId] =
    Encoder.encodeString
      .contramap(id => orgIdShow.show(id))

  final implicit val orgIdDecoder: Decoder[OrgId] =
    Decoder.decodeString.emap {
      case regex(orgId) =>
        Right(OrgId(orgId))
      case _ =>
        Left("Unable to decode value into a OrgId")
    }

}
