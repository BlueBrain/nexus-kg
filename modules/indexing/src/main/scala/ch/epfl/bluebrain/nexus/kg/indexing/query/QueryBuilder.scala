package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import cats.Show
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.IndexingVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.{QueryParams, TotalCount}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.SelectField.{SelectExpr, SelectVar}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.WhereField._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilderStages._
import org.apache.jena.arq.querybuilder.SelectBuilder
import org.apache.jena.query.Query

trait QueryBuilder {
  /**
    * the provided parameters to build a SPARQL query
    *
    * @return the [[QueryParams]] instance
    */
  def params: QueryParams

  /**
    * Builds a [[Query]] using a [[QueryBuilder]] and the provided parameters
    *
    * @return the [[Query]] instance
    */
  def build(): Query = new SelectQueryBuilder(params).apply().build()
}


class SelectQueryBuilder private[query](override val params: QueryParams) extends QueryBuilder {

  import cats.syntax.show._
  import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.WhereField._

  private def applyWithoutTotal(): SelectBuilder = {
    val q = prepareQuery()

    //Add subquery
    params.subQueries.foreach(subQuery =>
      q.addSubQuery(subQuery.apply()))

    //Add union
    params.unions.foreach(union => q.addUnion(union.apply()))

    //Adding where
    params.where.reverse.foreach(field => q.addWhere(field.s.show, field.p.show, field.o.show))

    //Adding filter
    params.filter.map(q.addFilter(_))

    //Adding groupBy
    params.group.foreach(q.addGroupBy(_))

    //Adding distinct value
    q.setDistinct(params.distinct)

    //Add pagination
    params.pagination.map(p => {
      q.setOffset(p.from.toInt)
      q.setLimit(p.size)
    })

    q
  }

  private def prepareQuery(otherSelect: Option[String] = None): SelectBuilder = {
    val q = new SelectBuilder()

    //Adding prefix
    params.prefix.map(p => q.addPrefix(p.prefix, p.uri.toString()))

    //Adding select
    otherSelect.map(v => q.addVar(s"?$v"))
    params.select.map {
      case SelectVar(v)     => q.addVar(s"?$v")
      case SelectExpr(e, v) => q.addVar(e, v)
    }
    q
  }

  private def applyWithTotal(total: TotalCount): SelectBuilder = {
    val q = prepareQuery(Some(total.variable))

    val queryParams = params.copy(total = None)

    val countQueryParams = params.copy(group = Seq.empty, pagination = None, total = None,
      select = Seq(SelectExpr(total.expr, total.variable)))

    //Add sub queries
    q.addSubQuery(new SelectQueryBuilder(countQueryParams).apply())
    q.addUnion(new SelectQueryBuilder(queryParams).apply())
  }

  def apply(): SelectBuilder = params.total.map(applyWithTotal(_)).getOrElse(applyWithoutTotal())

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
  def select(values: SelectField*): Selects = new PrefixMappings() select (values: _*)

  /**
    * Holds all the provided parameters to build a SPARQL query.
    *
    * @param prefix     the parameters for the prefix step
    * @param select     the parameters for the select step
    * @param distinct   the parameter which defines if the select is distinct or not
    * @param where      the parameters for the where step
    * @param filter     the parameters for the filter step
    * @param group      the parameters for the groupBy step
    * @param subQueries the parameters for the subquery
    * @param unions     the parameters for the union step
    * @param pagination the parameters for the pagination step
    * @param total      the optional parameters which defines the field where to take the
    *                   total count from
    */
  private[query] final case class QueryParams(
    prefix: Seq[PrefixMapping] = Seq.empty,
    select: Seq[SelectField] = Seq.empty,
    distinct: Boolean = false,
    where: Seq[Where[WhereField]] = Seq.empty,
    filter: Option[String] = None,
    group: Seq[String] = Seq.empty,
    subQueries: Seq[SelectQueryBuilder] = Seq.empty,
    unions: Seq[SelectQueryBuilder] = Seq.empty,
    pagination: Option[Pagination] = None,
    total: Option[TotalCount] = None)

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
  }

  /**
    * Data type representing a triple on the where clause of the SPARQL query.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    */
  final case class Where[+A <: WhereField](s: A, p: A, o: A)

  object Where {
    /**
      * Constructs a where from a tuple with a default subject "s"
      * This where field will filter for the predicate as a value and the subject and object as variables.
      *
      * @param tuple a tuple for the predicate and the object
      * @return a triple for the where clause
      */
    implicit def apply(tuple: (String, String)): Where[WhereField] = Where[WhereField](WhereVar(subject), WhereVal(tuple._1), WhereVar(tuple._2))

    /**
      * Constructs a where from [[WhereField]]s
      *
      * @param triple the subject, predicate and object in a [[Tuple3]]
      * @return a triple for the where clause
      */
    implicit def applyTriple[A <: WhereField, B <: WhereField, C <: WhereField](triple: (A, B, C)): Where[WhereField] = Where[WhereField](triple._1, triple._2, triple._3)

    /**
      * Constructs a where from a tuple with a default subject "s"
      * This where field will filter for the predicate as a Uri and the subject and object as variables.
      *
      * @param tuple a tuple for the predicate and the object
      * @return a triple for the where clause
      */
    implicit def applyUri(tuple: (Uri, String)): Where[WhereField] = Where[WhereField](WhereVar(subject), WhereUri(tuple._1.toString()), WhereVar(tuple._2))

  }

  /**
    * Data type representing the select field.
    *
    */
  sealed trait SelectField {
    def variable: String
  }

  object SelectField {

    /**
      * Data type representing a select field which is evaluated as an expression with a variable
      *
      * @param expr     the expression of this select field
      * @param variable the variable how the expression is mapped
      */
    final case class SelectExpr(val expr: String, override val variable: String) extends SelectField

    /**
      * Data type representing a select field which is evaluated as a variable
      *
      * @param variable the variable of the select field
      */
    final case class SelectVar(override val variable: String) extends SelectField

    /**
      * Constructs a [[SelectField]] of subtype [[SelectVar]] which is evaluated as a variable.
      *
      * @param variable a select treated as a variable
      * @return the [[SelectField]] of a subtype [[SelectVar]]
      */
    implicit def apply(variable: String): SelectField = SelectVar(variable)

    /**
      * Constructs a [[SelectField]] of subtype [[SelectExpr]] which is evaluated as an expression with a variable.
      *
      * @param tuple the expression and the variable where the expression result will be stored
      *              wrapped in a [[Tuple2]]
      * @return the [[SelectField]] of a subtype [[SelectExpr]]
      */
    implicit def applySelectExpr(tuple: (String, String)): SelectField = SelectExpr(tuple._1, tuple._2)

  }

  /**
    * Data type representing one of the triple fields.
    *
    */
  sealed trait WhereField {
    def key: String
  }

  object WhereField {

    /**
      * Data type representing a triple field which has to be evaluated as a value.
      *
      * @param key the value of the field
      */
    final case class WhereVal(override val key: String) extends WhereField

    /**
      * Data type representing a triple field which has to be evaluated as a variable.
      *
      * @param key the key of the variable
      */
    final case class WhereVar(override val key: String) extends WhereField

    /**
      * Data type representing a triple field which has to be evaluated as a uri.
      *
      * @param key the key of the variable
      */
    final case class WhereUri(override val key: String) extends WhereField

    /**
      * Implements Show for all subtypes of [[WhereField]]
      */
    implicit val showWhereField: Show[WhereField] = Show.show {
      case WhereVal(key) => key
      case WhereVar(key) => s"?$key"
      case WhereUri(key) => s"<$key>"

    }
  }

  /**
    * Data type representing the total count and the field used for it.
    *
    * @param countField the field to which the counting happens
    * @param variable   the variable where the count result is stored
    */
  final case class TotalCount(private val countField: String, variable: String) {
    val expr = s"COUNT(?$countField)"
  }

  object TotalCount {
    /**
      * Constructs a [[TotalCount]] from a tuple
      *
      * @param tuple the tuple containing the field to which the counter happens
      *              and the variable where the count result is stored.
      * @return the [[TotalCount]] instance
      */
    implicit final def apply(tuple: (String, String)): TotalCount = TotalCount(tuple._1, tuple._2)

  }

}
