package ch.epfl.bluebrain.nexus.kg.indexing.filtering

/**
  * Enumeration type for all filtering operations.
  */
sealed trait Op extends Product with Serializable

object Op {

  /**
    * Enumeration type for all filtering logical operators.
    */
  sealed trait LogicalOp extends Op
  final case object And extends LogicalOp
  final case object Or extends LogicalOp
  final case object Not extends LogicalOp
  final case object Xor extends LogicalOp

  object LogicalOp {
    /**
      * Attempts to construct ''LogicalOp'' from the provided string.
      *
      * @param string the string representation of the logical operator
      */
    final def fromString(string: String): Option[LogicalOp] = string match {
      case "and" => Some(And)
      case "or"  => Some(Or)
      case "not" => Some(Not)
      case "xor" => Some(Xor)
      case _     => None
    }
  }

  /**
    * Enumeration type for all filtering comparison operators.
    */
  sealed trait ComparisonOp extends Op
  final case object Eq extends ComparisonOp
  final case object Ne extends ComparisonOp
  final case object Lt extends ComparisonOp
  final case object Gt extends ComparisonOp
  final case object Lte extends ComparisonOp
  final case object Gte extends ComparisonOp

  object ComparisonOp {

    /**
      * Attempts to construct ''ComparisonOp'' from the provided string.
      *
      * @param string the string representation of the comparison operator
      */
    def fromString(string: String): Option[ComparisonOp] = string match {
      case "eq"  => Some(Eq)
      case "ne"  => Some(Ne)
      case "lt"  => Some(Lt)
      case "lte" => Some(Lte)
      case "gt"  => Some(Gt)
      case "gte" => Some(Gte)
      case _     => None
    }
  }

  /**
    * Operator to represent value checks within a collection.
    */
  sealed trait In extends Op
  final case object In extends In {
    /**
      * Attempts to construct ''In'' from the provided string.
      *
      * @param string the string representation of the ''In'' operator
      */
    def fromString(string: String): Option[In] =
      if (string == "in") Some(In) else None
  }
}