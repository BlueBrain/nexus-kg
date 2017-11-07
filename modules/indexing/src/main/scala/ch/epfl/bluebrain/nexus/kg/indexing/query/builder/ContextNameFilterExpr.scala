package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextName
import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.UriTerm

/**
  * Defines the creation of an expression to target specific context names.
  */
trait ContextNameFilterExpr {

  /**
    * Creates an expression to target context name resources for ''Id''
    */
  def apply(contextName: ContextName)(implicit Q: ConfiguredQualifier[String],
                                      contextNameNameQ: ConfiguredQualifier[ContextName]): Expr
}

object ContextNameFilterExpr {

  val contextNameFilterExpr: ContextNameFilterExpr {
    def apply(contextName: ContextName)(implicit Q: ConfiguredQualifier[String],
                                        contextNameQ: ConfiguredQualifier[ContextName]): ComparisonExpr
  } = new ContextNameFilterExpr {
    override def apply(contextName: ContextName)(implicit Q: ConfiguredQualifier[String],
                                                 contextNameQ: ConfiguredQualifier[ContextName]) =
      ComparisonExpr(Eq, UriPath(contextGroupKey), UriTerm(contextName qualify))
  }

}
