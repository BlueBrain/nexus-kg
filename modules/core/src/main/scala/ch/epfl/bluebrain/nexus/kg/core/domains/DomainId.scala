package ch.epfl.bluebrain.nexus.kg.core.domains

import cats.Show
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId._
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex

/**
  * A domain identifier rooted in an organization.
  *
  * @param orgId the unique organization identifier
  * @param id    the unique domain identifier
  */
final case class DomainId(orgId: OrgId, id: String)

object DomainId {
  final val regex: Regex = s"${OrgId.regex.regex}/([a-zA-Z0-9]+)".r

  final implicit val domainIdShow: Show[DomainId] = Show.show { id =>
    s"${id.orgId.show}/${id.id}"
  }

  final implicit val domainIdEncoder: Encoder[DomainId] =
    Encoder.encodeString
      .contramap(id => domainIdShow.show(id))

  final implicit val domainIdDecoder: Decoder[DomainId] =
    Decoder.decodeString.emap {
      case regex(orgId, domId) => Right(DomainId(OrgId(orgId), domId))
      case _                   => Left("Unable to decode value into a DomainId")
    }
}
