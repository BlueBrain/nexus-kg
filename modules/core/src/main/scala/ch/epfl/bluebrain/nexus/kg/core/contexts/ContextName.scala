package ch.epfl.bluebrain.nexus.kg.core.contexts

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId

import scala.util.matching.Regex

/**
  * Unique context name identifier rooted in a domain.
  *
  * @param domainId the domain identifier for this context
  * @param name     the name of the context
  */
final case class ContextName(domainId: DomainId, name: String) {

  /**
    * Constructs a [[ContextId]] from a the current [[ContextName]] with a provided ''version''.
    *
    * @param version the version of the context
    */
  def versioned(version: Version): ContextId = ContextId(domainId, name, version)
}

object ContextName {
  final val regex: Regex = s"""${DomainId.regex.regex}/([a-z0-9]{2,32})""".r

  /**
    * Attempts to parse the argument string into a ''ContextName''.
    *
    * @param string the string representation of the context name.
    * @return either a ''Some[ContextName]'' if the string matches the expected format or a ''None'' otherwise
    */
  final def apply(string: String): Option[ContextName] = string match {
    case regex(org, dom, name) => Some(ContextName(DomainId(OrgId(org), dom), name))
    case _                     => None
  }

  final implicit def contextNameShow(implicit D: Show[DomainId]): Show[ContextName] = Show.show { name =>
    s"${name.domainId.show}/${name.name}"
  }
}
