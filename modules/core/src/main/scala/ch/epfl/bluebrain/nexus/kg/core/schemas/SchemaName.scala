package ch.epfl.bluebrain.nexus.kg.core.schemas

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId

import scala.util.matching.Regex

/**
  * Unique schema name identifier rooted in a domain.
  *
  * @param domainId the domain identifier for this schema
  * @param name     the name of the schema
  */
final case class SchemaName(domainId: DomainId, name: String) {

  /**
    * Constructs a [[SchemaId]] from a the current [[SchemaName]] with a provided ''version''.
    *
    * @param version the version of the schema
    */
  def versioned(version: Version): SchemaId = SchemaId(domainId, name, version)
}

object SchemaName {
  final val regex: Regex = s"""${DomainId.regex.regex}/([a-zA-Z0-9]+)""".r

  /**
    * Attempts to parse the argument string into a ''SchemaName''.
    *
    * @param string the string representation of the schema name.
    * @return either a ''Some[SchemaName]'' if the string matches the expected format or a ''None'' otherwise
    */
  final def apply(string: String): Option[SchemaName] = string match {
    case regex(org, dom, name) =>
      Some(SchemaName(DomainId(OrgId(org), dom), name))
    case _ => None
  }

  final implicit def schemaNameShow(
      implicit D: Show[DomainId]): Show[SchemaName] = Show.show { name =>
    s"${name.domainId.show}/${name.name}"
  }
}
