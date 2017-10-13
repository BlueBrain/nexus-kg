package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.{SeqPath, UriPath}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.UriTerm

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
}
