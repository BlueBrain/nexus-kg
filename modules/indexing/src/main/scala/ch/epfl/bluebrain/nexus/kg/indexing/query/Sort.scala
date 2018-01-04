package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.indexing.query.Sort.OrderType
import ch.epfl.bluebrain.nexus.kg.indexing.query.Sort.OrderType._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SortList._

import scala.util.Try

/**
  * Data type of a collection of [[Sort]]
  * @param values the values to be sorted in the provided order
  */
final case class SortList(values: List[Sort]) extends Product with Serializable {
  private lazy val toVarsMapping = values.zipWithIndex.map { case (sort, i) => sort -> s"?sort$i" }

  /**
    * @return the string representation of the variables used for sorting (inside the SELECT clause) in SPARQL language
    */
  def toVars = toVarsMapping map (_._2) mkString (" ")

  /**
    * @return the string representation of the optional triples used for sorting (inside the WHERE clause) in SPARQL language
    */
  def toTriples =
    toVarsMapping map {
      case (Sort(_, predicate), variable) => s"?s <${predicate}> $variable"
    } mkString ("}\nOPTIONAL{") prefixSuffixNonEmpty ("OPTIONAL{", "}")

  /**
    * @return the string representation of the ORDER BY clause used for sorting in SPARQL language
    */
  def toOrderByClause =
    Option(toVarsMapping map {
      case (Sort(Desc, _), variable) => s"DESC($variable)"
      case (_, variable)             => s"ASC($variable)"
    }).filterNot(_.isEmpty).getOrElse(List("?s")) mkString (" ")
}

object SortList {
  val Empty = SortList(List.empty)
  private[query] implicit class StringSyntax(value: String) {
    def prefixSuffixNonEmpty[A](prefix: String, suffix: String): String =
      if (value.trim.isEmpty) value else prefix + value + suffix
  }

}

/**
  * Data type of a ''value'' to be sorted
  *
  * @param order the order (ascending or descending) of the sorting value
  * @param value the value to be sorted
  */
final case class Sort(order: OrderType, value: Uri)

object Sort {

  private def validAbsoluteUri(value: String): Option[Uri] =
    Try(Uri(value)).filter(uri => uri.isAbsolute && uri.toString().indexOf("/") > -1).toOption

  /**
    * Attempt to construct a [[Sort]] from a string
    *
    * @param value the string
    */
  final def apply(value: String): Option[Sort] =
    value take 1 match {
      case "-" => validAbsoluteUri(value.drop(1)).map(Sort(Desc, _))
      case "+" => validAbsoluteUri(value.drop(1)).map(Sort(Asc, _))
      case _   => validAbsoluteUri(value).map(Sort(Asc, _))
    }

  /**
    * Enumeration type for all possible ordering
    */
  sealed trait OrderType extends Product with Serializable

  object OrderType {

    /**
      * Descending ordering
      */
    final case object Desc extends OrderType

    /**
      * Ascending ordering
      */
    final case object Asc extends OrderType

  }

}
