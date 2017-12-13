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
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings

abstract class AclSparqlExpr[Id](implicit S: QuerySettings) {

  /**
    * Constructs the sparQL union triples for filtering ACLs
    *
    * @param identities the [[Identity]] list that will be checked for read permissions on a particular resource
    * @param Q          the implicitly available [[ConfiguredQualifier]] for String
    */
  def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]): String

  /**
    * Syntax sugar to add API method to  a tuple of [[List]]
    *
    * @param tuple the tuple which contains the list of variables and the list from where to generate a UNION string
    * @tparam A the generic list type
    */
  implicit class MkUnionStringSytax[A](tuple: (List[A], List[A])) {
    def format: String = tuple match {
      case (vars, list @ _ :: _) =>
        vars.mkString("", " .\n", "\n") + list.mkString(s"GRAPH <${S.aclGraph}> { {", "\n}UNION{\n", "}}")
      case _ => ""
    }
  }
}

object AclSparqlExpr {

  private def orgTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    identities.flatMap { identity =>
      List(
        s"<${"root".qualifyAsString}> <$readKey> <${identity.id.show}>",
        s"?s <$readKey> <${identity.id.show}>"
      )
    }

  private def domainTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    orgTriples(identities) ++ identities.map { identity =>
      s"?orgId <$readKey> <${identity.id.show}>"
    }

  private def schemaNameTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    domainTriples(identities) ++ identities.map { identity =>
      s"?domainId <$readKey> <${identity.id.show}>"
    }

  private def schemaTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.map { identity =>
      s"?schemaGroupId <$readKey> <${identity.id.show}>"
    }

  private def contextTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.map { identity =>
      s"?contextGroupId <$readKey> <${identity.id.show}>"
    }

  private def instanceTriples(identities: List[Identity])(implicit Q: ConfiguredQualifier[String]) =
    schemaNameTriples(identities) ++ identities.flatMap { identity =>
      List(
        s"?schemaId <$readKey> <${identity.id.show}>",
        s"?schemaGroupId <$readKey> <${identity.id.show}>"
      )
    }

  private def domainBringVars(implicit Q: ConfiguredQualifier[String]) =
    List(s"?s <${"organization".qualifyAsString}> ?orgId")

  private def schemaNameBringVars(implicit Q: ConfiguredQualifier[String]) =
    s"?s <${"domain".qualifyAsString}> ?domainId" :: domainBringVars

  private def schemaBringVars(implicit Q: ConfiguredQualifier[String]) =
    s"?s <$schemaGroupKey> ?schemaGroupId" :: schemaNameBringVars

  private def contextBringVars(implicit Q: ConfiguredQualifier[String]) =
    s"?s <$contextGroupKey> ?contextGroupId" :: schemaNameBringVars

  private def instanceBringVars(implicit Q: ConfiguredQualifier[String]) =
    schemaNameBringVars ++ List(
      s"?s <${"schema".qualifyAsString}> ?schemaId",
      s"?s <${"schema".qualifyAsString}>/<$schemaGroupKey> ?schemaGroupId",
    )

  implicit def orgIdAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[OrgId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) = {
      (List.empty[String], orgTriples(identities.toList)).format
    }
  }

  implicit def domainIdAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[DomainId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) = {
      (domainBringVars, domainTriples(identities.toList)).format
    }
  }

  implicit def schemaNameAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[SchemaName] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      (schemaNameBringVars, schemaNameTriples(identities.toList)).format
  }

  implicit def contextNameAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[ContextName] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      (schemaNameBringVars, schemaNameTriples(identities.toList)).format
  }

  implicit def schemaIdAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[SchemaId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      (schemaBringVars, schemaTriples(identities.toList)).format
  }

  implicit def contextIdAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[ContextId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      (contextBringVars, contextTriples(identities.toList)).format
  }

  implicit def instanceIdAclInstance(implicit S: QuerySettings) = new AclSparqlExpr[InstanceId] {
    override def apply(identities: Set[Identity])(implicit Q: ConfiguredQualifier[String]) =
      (instanceBringVars, instanceTriples(identities.toList)).format
  }
}
