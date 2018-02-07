package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.{SeqPath, UriPath}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.UriTerm

/**
  * Defines the creation of an expression to target specific schema names.
  *
  * @tparam Id the generic type which defines the targeted resource
  */
trait SchemaNameFilterExpr[Id] {

  /**
    * Creates an expression to target schema name resources for ''Id''
    */
  def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                    schemaNameQ: ConfiguredQualifier[SchemaName]): Expr
}

object SchemaNameFilterExpr {

  implicit val schemaFilterExpr = new SchemaNameFilterExpr[SchemaId] {
    override def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                               schemaNameQ: ConfiguredQualifier[SchemaName]) =
      ComparisonExpr(Eq, UriPath(schemaGroupKey), UriTerm(schemaName qualify))
  }

  implicit val instanceFilterExpr = new SchemaNameFilterExpr[InstanceId] {
    override def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                               schemaNameQ: ConfiguredQualifier[SchemaName]) =
      ComparisonExpr(Eq, SeqPath(UriPath("schema" qualify), UriPath(schemaGroupKey)), UriTerm(schemaName qualify))
  }

  implicit val orgFilterExpr = new SchemaNameFilterExpr[OrgId] {
    override def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                               schemaNameQ: ConfiguredQualifier[SchemaName]): Expr = Expr.NoopExpr
  }

  implicit val domainFilterExpr = new SchemaNameFilterExpr[DomainId] {
    override def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                               schemaNameQ: ConfiguredQualifier[SchemaName]): Expr = Expr.NoopExpr
  }

  implicit val contextFilterExpr = new SchemaNameFilterExpr[ContextId] {
    override def apply(schemaName: SchemaName)(implicit Q: ConfiguredQualifier[String],
                                               schemaNameQ: ConfiguredQualifier[SchemaName]): Expr = Expr.NoopExpr
  }
}
