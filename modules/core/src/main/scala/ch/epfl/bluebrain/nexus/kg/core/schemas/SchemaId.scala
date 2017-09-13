package ch.epfl.bluebrain.nexus.kg.core.schemas

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex
import scala.util.{Failure, Try}

/**
  * Unique schema identifier rooted in a domain.
  *
  * @param domainId the domain identifier for this schema
  * @param name     the name of the schema
  * @param version  the version of the schema
  */
final case class SchemaId(domainId: DomainId, name: String, version: Version)

object SchemaId {
  final val regex: Regex = s"""${DomainId.regex.regex}/([a-zA-Z0-9]+)/v([0-9]+)\\.([0-9]+)\\.([0-9]+)""".r

  /**
    * Attempts to parse the argument string into a ''SchemaId''.
    *
    * @param string the string representation of the schema id.
    * @return either a ''Success[SchemaId]'' or a ''Failure'' wrapping an [[IllegalArgumentException]]
    */
  final def apply(string: String): Try[SchemaId] = string match {
    case regex(org, dom, name, major, minor, patch) =>
      Try(Version(major.toInt, minor.toInt, patch.toInt)).map { ver =>
        SchemaId(DomainId(OrgId(org), dom), name, ver)
      }
    case _                                          =>
      Failure(new IllegalArgumentException("Unable to decode value into a SchemaId"))
  }

  final implicit def schemaIdShow(implicit D: Show[DomainId], V: Show[Version]): Show[SchemaId] = Show.show { id =>
    s"${id.domainId.show}/${id.name}/${id.version.show}"
  }

  final implicit def schemaIdEncoder(implicit S: Show[SchemaId]): Encoder[SchemaId] =
    Encoder.encodeString.contramap(id => S.show(id))

  final implicit val schemaIdDecoder: Decoder[SchemaId] =
    Decoder.decodeString.emapTry(str => apply(str))
}