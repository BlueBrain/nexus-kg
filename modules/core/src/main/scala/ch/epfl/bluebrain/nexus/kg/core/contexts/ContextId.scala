package ch.epfl.bluebrain.nexus.kg.core.contexts

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex
import scala.util.{Failure, Try}

final case class ContextId(domainId: DomainId, name: String, version: Version) {
  /**
    * Extracts the [[ContextName]] from the current [[ContextId]].
    */
  def contextName: ContextName = ContextName(domainId, name)
}

object ContextId {

  final val regex: Regex =
    s"""${ContextName.regex.regex}/v([0-9]+)\\.([0-9]+)\\.([0-9]+)""".r

  /**
    * Attempts to parse the argument string into a ''ContextId''.
    *
    * @param string the string representation of the context id.
    * @return either a ''Success[ContextId]'' or a ''Failure'' wrapping an [[IllegalArgumentException]]
    */
  final def apply(string: String): Try[ContextId] = string match {
    case regex(org, dom, name, major, minor, patch) =>
      Try(Version(major.toInt, minor.toInt, patch.toInt)).map { ver =>
        ContextId(DomainId(OrgId(org), dom), name, ver)
      }
    case _                                          =>
      Failure(new IllegalArgumentException("Unable to decode value into a ContextId"))
  }

  final implicit def contextIdShow(implicit D: Show[ContextName], V: Show[Version]): Show[ContextId] =
    Show.show { id =>
      s"${id.contextName.show}/${id.version.show}"
    }

  final implicit def contextIdEncoder(implicit S: Show[ContextId]): Encoder[ContextId] =
    Encoder.encodeString.contramap(id => S.show(id))

  final implicit val contextIdDecoder: Decoder[ContextId] =
    Decoder.decodeString.emapTry(str => apply(str))


}