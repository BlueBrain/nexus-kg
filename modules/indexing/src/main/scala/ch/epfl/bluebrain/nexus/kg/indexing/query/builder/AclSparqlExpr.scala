package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._

trait AclSparqlExpr[Id] {

  /**
    * Constructs the sparQL union triples for filtering ACLs
    *
    * @param identities the [[Identity]] list that will be checked for read permissions on a particular resource
    * @param Q          the implicitly available [[ConfiguredQualifier]] for String
    */
  def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]): String

}

object AclSparqlExpr {

  /**
    * Syntax sugar to add API method to ''list''
    *
    * @param list the list from where to generate a UNION string
    * @tparam A the generic list type
    */
  private implicit class MkUnionStringSytax[A](list: List[A]) {
    def mkUnionString: String = list match {
      case _ :: _ => list.mkString("{", "\n}UNION{\n", "}")
      case _      => ""
    }
  }

  private def orgTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    identities.flatMap { identity =>
      List(
        s"<${identity.id.show}> <$hasPermissionsKey> <$readAllTypeKey>",
        s"?s <$readKey> <${identity.id.show}>",
      )
    }

  private def domainTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    orgTriples(identities) ++ identities.map { identity =>
      s"?s <${"organization".qualifyAsString}>/<$readKey> <${identity.id.show}>"
    }

  private def schemaNameTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    domainTriples(identities) ++ identities.map { identity =>
      s"?s <${"domain".qualifyAsString}>/<$readKey> <${identity.id.show}>"
    }

  private def schemaTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.map { identity =>
      s"?s <$schemaGroupKey>/<$readKey> <${identity.id.show}>"
    }

  private def contextTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.map { identity =>
      s"?s <$contextGroupKey>/<$readKey> <${identity.id.show}>"
    }

  private def instanceTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.flatMap { identity =>
      List(
        s"?s <${"schema".qualifyAsString}>/<$readKey> <${identity.id.show}>",
        s"?s <${"schema".qualifyAsString}>/<$schemaGroupKey>/<$readKey> <${identity.id.show}>",
      )
    }

  implicit def orgIdAclInstance = new AclSparqlExpr[OrgId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      orgTriples(identities.toList).mkUnionString
  }

  implicit def domainIdAclInstance = new AclSparqlExpr[DomainId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      domainTriples(identities.toList).mkUnionString
  }

  implicit def schemaNameAclInstance = new AclSparqlExpr[SchemaName] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      schemaNameTriples(identities.toList).mkUnionString
  }

  implicit def contextNameAclInstance = new AclSparqlExpr[ContextName] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      schemaNameTriples(identities.toList).mkUnionString
  }

  implicit def schemaIdAclInstance = new AclSparqlExpr[SchemaId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      schemaTriples(identities.toList).mkUnionString
  }

  implicit def contextIdAclInstance = new AclSparqlExpr[ContextId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      contextTriples(identities.toList).mkUnionString
  }

  implicit def instanceIdAclInstance = new AclSparqlExpr[InstanceId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      instanceTriples(identities.toList).mkUnionString
  }
}
