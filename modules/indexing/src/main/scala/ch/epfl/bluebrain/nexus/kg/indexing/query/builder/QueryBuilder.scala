package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import akka.http.scaladsl.model.Uri
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field.{Var, _}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.TripleContent._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilderStages._

trait QueryBuilder {
  /**
    * the provided parameters to build a Blazegraph query
    *
    * @return the [[QueryParams]] instance
    */
  def params: QueryParams

  /**
    * Builds a [[Query]] using a [[QueryBuilder]] and the provided parameters
    *
    * @return the [[Query]] instance
    */
  def build(): Query = new ParamsQueryBuilder(params).apply()

  def buildCount(field: String = subject): Query = {

    val resultSet = "resultSet"

    val withParams = params.copy(prefix = Seq.empty, order = Seq.empty, pagination = None) -> resultSet

    val countQueryParams = QueryParams(select = Seq(count()), includes = resultSet +: params.includes)

    val unionQueryParams = params.copy(prefix = Seq.empty, includes = resultSet +: params.includes, select = Seq(Expr("*", None)))

    val finalParams =
      QueryParams(select = Var(total) +: params.select, withs = withParams +: params.withs, prefix = params.prefix, subQueries = Seq(countQueryParams), unions = Seq(unionQueryParams))

    ParamsQueryBuilder(finalParams).apply()
  }

}

object QueryBuilder {

  /**
    * Constructs the [[PrefixMappings]] step of the query builder form one prefix.
    *
    * @param tuple a provided prefix
    * @return the [[PrefixMappings]] instance
    */
  def prefix(tuple: (String, Uri)): PrefixMappings = new PrefixMappings(tuple)

  /**
    * Constructs the [[Selects]] step of the query builder
    *
    * @param values the provided selects
    * @return the [[Selects]] instance
    */
  def select(values: Field*): Selects = new PrefixMappings() select (values: _*)

  /**
    * Constructs the [[Selects]] step of the query builder returning all fields
    *
    * @return the [[Selects]] instance
    */
  def selectAll: Selects = new PrefixMappings() select (Expr("*", None))

  /**
    * Constructs the [[Selects]] step of the query builder with the DISTINCT clause
    *
    * @param values the provided selects
    * @return the [[Selects]] instance
    */
  def selectDistinct(values: Field*): Selects = new PrefixMappings() selectDistinct (values: _*)

  /**
    * Data type representing a prefix mapping.
    *
    * @param prefix the prefix which substitutes the uri
    * @param uri    the prefix URI
    */
  final case class PrefixMapping(prefix: String, uri: Uri)

  object PrefixMapping {
    /**
      * Constructs a prefix from a tuple
      *
      * @param tuple a tuple with [[String]] and [[akka.http.scaladsl.model.Uri]]
      * @return an instance of [[PrefixMapping]]
      */
    implicit def apply(tuple: (String, Uri)): PrefixMapping = PrefixMapping(tuple._1, tuple._2)

    implicit val showPrefixMapping: Show[PrefixMapping] =
      Show.show(p => s"PREFIX ${p.prefix}: <${p.uri}>")
  }

  /**
    * Data type representing a triple.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    */
  final case class Triple[+A <: TripleContent](s: A, p: A, o: A)

  object Triple {
    /**
      * Constructs a where from a tuple with a default subject "s"
      * This where field will filter for the predicate as a value and the subject and object as variables.
      *
      * @param tuple a tuple for the predicate and the object
      * @return a triple for the where clause
      */
    implicit def apply(tuple: (String, String)): Triple[TripleContent] = Triple[TripleContent](VarContent(subject), ValContent(tuple._1), VarContent(tuple._2))

    /**
      * Constructs a where from [[TripleContent]]s
      *
      * @param triple the subject, predicate and object in a [[Tuple3]]
      * @return a triple for the where clause
      */
    implicit def applyTriple[A <: TripleContent, B <: TripleContent, C <: TripleContent](triple: (A, B, C)): Triple[TripleContent] = Triple[TripleContent](triple._1, triple._2, triple._3)

    /**
      * Constructs a where from a tuple with a default subject "s"
      * This where field will filter for the predicate as a Uri and the subject and object as variables.
      *
      * @param tuple a tuple for the predicate and the object
      * @return a triple for the where clause
      */
    implicit def applyUri(tuple: (Uri, String)): Triple[TripleContent] = Triple[TripleContent](VarContent(subject), UriContent(tuple._1), VarContent(tuple._2))

    /**
      * Constructs a where from a tuple with a default subject "s"
      * This where field will filter for the predicate as a query and the subject and object as variables.
      *
      * @param tuple a tuple for the predicate and the object
      * @return a triple for the where clause
      */
    implicit def applyQuery(tuple: (QueryContent, String)): Triple[TripleContent] = Triple[TripleContent](VarContent(subject), tuple._1, VarContent(tuple._2))

    implicit val showTriple: Show[Triple[TripleContent]] =
      Show.show(where => s"${where.s.show} ${where.p.show} ${where.o.show} .")
  }

  /**
    * Data type representing the select or a group by field.
    *
    */
  sealed trait Field extends Product with Serializable

  object Field {

    final def count(tuple: (String, String) = subject -> total): Field = (s"COUNT(DISTINCT ?${tuple._1})" -> tuple._2)

    /**
      * Data type representing a select or a group by field which is evaluated as an expression with a variable
      *
      * @param expr     the expression of this select field
      * @param variable the variable how the expression is mapped
      */
    final case class Expr(expr: String, variable: Option[String]) extends Field

    /**
      * Data type representing a select or a group by field which is evaluated as a variable
      *
      * @param variable the variable of the select field
      */
    final case class Var(variable: String) extends Field

    /**
      * Constructs a [[Field]] of subtype [[Var]] which is evaluated as a variable.
      *
      * @param variable a select or a group by treated as a variable
      * @return the [[Field]] of a subtype [[Var]]
      */
    implicit def apply(variable: String): Field = Var(variable)

    /**
      * Constructs a [[Field]] of subtype [[Expr]] which is evaluated as an expression with a variable.
      *
      * @param tuple the expression and the variable where the expression result will be stored
      *              wrapped in a [[Tuple2]]
      * @return the [[Field]] of a subtype [[Expr]]
      */
    implicit def applySelectExpr(tuple: (String, String)): Field = Expr(tuple._1, Some(tuple._2))

    implicit val showField: Show[Field] =
      Show.show {
        case Expr(expr, Some(v)) => s"($expr AS ?$v)"
        case Expr(expr, None) => s"$expr"
        case Var(v)        => s"?$v"
      }
  }

  /**
    * Data type representing one of the triple fields.
    *
    */
  sealed trait TripleContent extends Product with Serializable

  object TripleContent {

    /**
      * Data type representing a triple field which has to be evaluated as a value.
      *
      * @param key the value of the field
      */
    final case class ValContent[A](key: A) extends TripleContent

    /**
      * Data type representing a triple field which has to be evaluated as a variable.
      *
      * @param key the key of the variable
      */
    final case class VarContent(key: String) extends TripleContent

    /**
      * Data type representing a triple field which has to be evaluated as a uri.
      *
      * @param key the key of the variable
      */
    final case class UriContent(key: Uri) extends TripleContent

    /**
      * Data type representing a triple field which has to be evaluated as a query.
      *
      * @param key the key of the query
      */
    final case class QueryContent(key: String) extends TripleContent

    /**
      * Implements Show for all subtypes of [[TripleContent]]
      */
    implicit val showWhereField: Show[TripleContent] = Show.show {
      case VarContent(key)           => s"?$key"
      case UriContent(key)           => s"<$key>"
      case QueryContent(key)         => s"$key"
      case ValContent(value: String) => s""""$value""""
      case ValContent(any)           => s"$any"
    }
  }

  sealed trait Order extends Product with Serializable

  object Order {

    final case class Asc(value: Field) extends Order

    final case class Desc(value: Field) extends Order

    implicit def apply(value: String): Order = Asc(value)

    implicit val showOrder: Show[Order] = Show.show {
      case Asc(value)  => s"ASC(${value.show})"
      case Desc(value) => s"DESC(${value.show})"
    }
  }

}