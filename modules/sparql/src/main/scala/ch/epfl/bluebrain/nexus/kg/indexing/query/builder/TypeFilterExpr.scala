package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.UriTerm

/**
  * Defines the creation of a type expression to target specific type resources.
  *
  * @tparam Id the generic type which defines the targeted resource
  */
trait TypeFilterExpr[Id] {

  /**
    * Creates an expression to target type resources for ''Id''
    */
  def apply(implicit Q: ConfiguredQualifier[String]): Expr
}

object TypeFilterExpr {

  implicit val orgFilterExpr = new TypeFilterExpr[OrgId] {
    override def apply(implicit Q: ConfiguredQualifier[String]) =
      ComparisonExpr(Eq, UriPath(rdfTypeKey), UriTerm("Organization".qualify))
  }

  implicit val domainFilterExpr = new TypeFilterExpr[DomainId] {
    override def apply(implicit Q: ConfiguredQualifier[String]) =
      ComparisonExpr(Eq, UriPath(rdfTypeKey), UriTerm("Domain".qualify))
  }

  implicit val schemaFilterExpr = new TypeFilterExpr[SchemaId] {
    override def apply(implicit Q: ConfiguredQualifier[String]) =
      ComparisonExpr(Eq, UriPath(rdfTypeKey), UriTerm("Schema".qualify))
  }

  implicit val contextFilterExpr = new TypeFilterExpr[ContextId] {
    override def apply(implicit Q: ConfiguredQualifier[String]) =
      ComparisonExpr(Eq, UriPath(rdfTypeKey), UriTerm("Context".qualify))
  }

  implicit val instanceFilterExpr = new TypeFilterExpr[InstanceId] {
    override def apply(implicit Q: ConfiguredQualifier[String]) =
      ComparisonExpr(Eq, UriPath(rdfTypeKey), UriTerm("Instance".qualify))
  }
}
