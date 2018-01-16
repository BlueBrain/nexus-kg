package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.commons.types.search.Sort.OrderType._
import ch.epfl.bluebrain.nexus.commons.types.search._

object SortListSparql {
  private[query] implicit class StringSyntax(value: String) {
    def prefixSuffixNonEmpty[A](prefix: String, suffix: String): String =
      if (value.trim.isEmpty) value else prefix + value + suffix
  }

  /**
    * Interface syntax to expose new functionality into SortList type
    *
    * @param sort the [[SortList]] instance
    */
  implicit class SortListSyntax(sort: SortList) {
    private lazy val toVarsMapping = sort.values.zipWithIndex.map { case (sort, i) => sort -> s"?sort$i" }

    /**
      * @return the string representation of the variables used for sorting (inside the SELECT clause) in SPARQL language
      */
    def toVars = toVarsMapping.map { case (_, variable) => variable }.mkString(" ")

    /**
      * @return the string representation of the optional triples used for sorting (inside the WHERE clause) in SPARQL language
      */
    def toTriples =
      toVarsMapping
        .map {
          case (Sort(_, predicate), variable) => s"?s <${predicate}> $variable"
        }
        .mkString("}\nOPTIONAL{") prefixSuffixNonEmpty ("OPTIONAL{", "}")

    /**
      * @return the string representation of the ORDER BY clause used for sorting in SPARQL language
      */
    def toOrderByClause =
      Option(toVarsMapping.map {
        case (Sort(Desc, _), variable) => s"DESC($variable)"
        case (_, variable)             => s"ASC($variable)"
      }).filterNot(_.isEmpty).getOrElse(List("?s")).mkString(" ")

  }
}
