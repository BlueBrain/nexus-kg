package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.{ComparisonOp, LogicalOp}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.UriTerm

/**
  * Enumeration type for all filtering expressions.
  */
sealed trait Expr extends Product with Serializable

object Expr {
  /**
    * A logical filtering expression.
    *
    * @param operator the operator to use for combining the expressions
    * @param operands the expressions to be combined
    */
  final case class LogicalExpr(operator: LogicalOp, operands: Set[Expr]) extends Expr
  /**
    * A comparison filtering expression.
    *
    * @param operator the operator to use when testing the value at the described path
    * @param path     the predicate filter
    * @param value    the expected value used in conjunction with the operator
    */
  final case class ComparisonExpr(operator: ComparisonOp, path: UriTerm, value: Term) extends Expr
}