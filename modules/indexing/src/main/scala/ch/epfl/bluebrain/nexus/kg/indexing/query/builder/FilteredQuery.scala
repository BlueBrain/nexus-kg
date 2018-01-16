package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.model.Uri
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, SortList}
import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, InExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.{And, _}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, TermCollection, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Filter, Term}
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixUri._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SortListSparql._

/**
  * Describes paginated queries based on filters.
  */
object FilteredQuery {

  /**
    * Constructs a Blazegraph query based on the provided filter and pagination settings that also computes the total
    * number of results.  The filter is applied on the subjects.
    *
    * @param filter     the filter on which the query is based on
    * @param pagination the pagination settings for the generated query
    * @param identities the [[Identity]] list that will be checked for read permissions on a particular resource
    * @param term       the optional full text search term
    * @param sort       the sorting values
    * @return a Blazegraph query based on the provided filter and pagination settings
    */
  def apply[Id](
      filter: Filter,
      pagination: Pagination,
      identities: Set[Identity],
      term: Option[String] = None,
      sort: SortList = SortList.Empty)(implicit Q: ConfiguredQualifier[String], aclExpr: AclSparqlExpr[Id]): String = {
    applyWithWhere(buildWhereFrom(filter.expr), pagination, term, identities, sort)
  }

  private def applyWithWhere[Id](
      where: String,
      pagination: Pagination,
      term: Option[String],
      identities: Set[Identity],
      sort: SortList)(implicit Q: ConfiguredQualifier[String], aclExpr: AclSparqlExpr[Id]): String = {
    val (selectTotal, selectWith, selectSubQuery) = buildSelectsFrom(term, sort)
    val (orderByUnion, orderByTotal)              = buildOrderByFrom(term, sort)

    s"""
       |PREFIX bds: <${bdsUri.toString()}>
       |$selectTotal
       |WITH {
       |  $selectWith
       |  WHERE {
       |${buildWhereFrom(term)}
       |$where
       |${sort.toTriples}
       |${aclExpr(identities)}
       |  }
       |${buildGroupByFrom(term)}
       |} AS %resultSet
       |WHERE {
       |  {
       |    $selectSubQuery
       |    WHERE { INCLUDE %resultSet }
       |  }
       |  UNION
       |  {
       |    SELECT *
       |    WHERE { INCLUDE %resultSet }
       |    $orderByUnion
       |    LIMIT ${pagination.size}
       |    OFFSET ${pagination.from}
       |  }
       |}
       |$orderByTotal""".stripMargin
  }

  /**
    * Constructs a Blazegraph query based on the provided filters and pagination settings that also computes the total
    * number of results.
    *
    * @param thisSubject  the qualified uri of the subject to be selected
    * @param targetFilter the filter to be applied to filter the objects
    * @param pagination   the pagination settings for the generated query
    * @param identities   the [[Identity]] list that will be checked for read permissions on a particular resource
    * @param term         the optional full text search term
    * @param sort         the sorting values
    */
  def outgoing[Id](
      thisSubject: Uri,
      targetFilter: Filter,
      pagination: Pagination,
      identities: Set[Identity],
      term: Option[String] = None,
      sort: SortList = SortList.Empty)(implicit Q: ConfiguredQualifier[String], aclExpr: AclSparqlExpr[Id]): String = {
    val where =
      s"""
         |?ss ?p ?$subject .
         |FILTER ( ?ss = <$thisSubject> )
         |${buildWhereFrom(targetFilter.expr)}
       """.stripMargin.trim
    applyWithWhere(where, pagination, term, identities, sort)
  }

  /**
    * Constructs a Blazegraph query based on the provided filters and pagination settings that also computes the total
    * number of results.
    *
    * @param thisObject   the qualified uri of the object to be selected
    * @param sourceFilter the filter to be applied to filter the subjects
    * @param pagination   the pagination settings for the generated query
    * @param identities   the [[Identity]] list that will be checked for read permissions on a particular resource
    * @param term         the optional full text search term
    * @param sort         the sorting values
    */
  def incoming[Id](
      thisObject: Uri,
      sourceFilter: Filter,
      pagination: Pagination,
      identities: Set[Identity],
      term: Option[String] = None,
      sort: SortList = SortList.Empty)(implicit Q: ConfiguredQualifier[String], aclExpr: AclSparqlExpr[Id]): String = {
    val where =
      s"""
         |?$subject ?p ?o .
         |FILTER ( ?o = <$thisObject> )
         |${buildWhereFrom(sourceFilter.expr)}
       """.stripMargin.trim
    applyWithWhere(where, pagination, term, identities, sort)
  }

  private final case class Stmt(stmt: String, filter: Option[String])

  private def buildSelectsFrom(term: Option[String], sort: SortList): (String, String, String) = {
    term
      .map(
        _ =>
          (
            s"SELECT DISTINCT ?$total ?$subject ?$maxScore ?$score ?$rank",
            s"SELECT DISTINCT ?$subject ${sort.toVars} (max(?rsv) AS ?$score) (max(?pos) AS ?$rank)",
            s"SELECT (COUNT(DISTINCT ?$subject) AS ?$total) (max(?$score) AS ?$maxScore)"
        ))
      .getOrElse(
        (
          s"SELECT DISTINCT ?$total ?$subject",
          s"SELECT DISTINCT ?$subject ${sort.toVars}".trim,
          s"SELECT (COUNT(DISTINCT ?$subject) AS ?$total)"
        )
      )
  }

  private def buildOrderByFrom(term: Option[String], sort: SortList): (String, String) =
    term
      .map(_ => (s"ORDER BY ${sort.toOrderByClause}", s"ORDER BY DESC(?$score)"))
      .getOrElse((s"ORDER BY ${sort.toOrderByClause}", ""))

  private def buildGroupByFrom(term: Option[String]): String =
    term.map(_ => s"GROUP BY ?$subject").getOrElse("")

  private def buildWhereFrom(term: Option[String]): String =
    term.map(term => s"""
                        |?$subject ?matchedProperty ?matchedValue .
                        |?matchedValue $bdsSearch "$term" .
                        |?matchedValue $bdsRelevance ?rsv .
                        |?matchedValue $bdsRank ?pos .
                        |FILTER ( !isBlank(?s) )
     """.stripMargin.trim).getOrElse("")

  private def buildWhereFrom(expr: Expr): String = {
    val varPrefix = "var"
    val atomicIdx = new AtomicInteger(0)

    def nextIdx(): Int =
      atomicIdx.incrementAndGet()

    // ?s :p ?var
    // FILTER (?var IN (term1, term2))
    def inExpr(expr: InExpr): Stmt = {
      val idx                                 = nextIdx()
      val variable                            = s"?${varPrefix}_$idx"
      val InExpr(path, TermCollection(terms)) = expr
      val stmt                                = s"?$subject ${path.show} $variable ."
      val filter                              = terms.map(_.show).mkString(s"$variable IN (", ", ", ")")
      Stmt(stmt, Some(filter))
    }

    // ?s :p ?var .
    // FILTER ( ?var op term )
    def compExpr(expr: ComparisonExpr, allowDirectFilter: Boolean = false): Stmt = {
      expr match {
        case ComparisonExpr(_: Eq.type, path, term: UriTerm) if allowDirectFilter =>
          Stmt(s"?$subject ${path.show} ${term.show} .", None)
        case ComparisonExpr(op, path, term) =>
          val idx      = nextIdx()
          val variable = s"?${varPrefix}_$idx"
          Stmt(s"?$subject ${path.show} $variable .", Some(s"$variable ${op.show} ${term.show}"))
      }
    }

    def fromStmts(op: LogicalOp, stmts: Vector[Stmt]): String = op match {
      case And =>
        val select = stmts.map(_.stmt).mkString("\n")
        val filter = stmts.collect { case Stmt(_, Some(f)) => f }.mkString(" && ") match {
          case ""    => None
          case other => Some(other)
        }
        s"""
           |$select
           |${filter.map(f => s"FILTER ( $f )").getOrElse("")}
           |""".stripMargin
      case Or =>
        val select = stmts.map(v => s"OPTIONAL { ${v.stmt} }").mkString("\n")
        val filter = stmts.collect { case Stmt(_, Some(f)) => f }.mkString(" || ")
        s"""
           |$select
           |FILTER ( $filter )
           |""".stripMargin
      case Not =>
        val select = stmts.map(_.stmt).mkString("\n")
        val filter = stmts.collect { case Stmt(_, Some(f)) => f }.mkString(" || ")
        s"""
           |$select
           |FILTER NOT EXISTS {
           |$select
           |FILTER ( $filter )
           |}
           |""".stripMargin
      case Xor =>
        val optSelect = stmts.map(v => s"OPTIONAL { ${v.stmt} }").mkString("\n")
        val filter    = stmts.collect { case Stmt(_, Some(f)) => f }.mkString(" || ")
        val select    = stmts.map(_.stmt).mkString("\n")
        s"""
           |$optSelect
           |FILTER ( $filter )
           |FILTER NOT EXISTS {
           |$select
           |FILTER ( $filter )
           |}
           |""".stripMargin

    }

    def fromExpr(expr: Expr, allowDirectFilter: Boolean): String = expr match {
      case NoopExpr          => s"?$subject ?p ?o ."
      case e: ComparisonExpr => compExpr(e, allowDirectFilter).show
      case e: InExpr         => inExpr(e).show
      case LogicalExpr(And, exprs) =>
        val (statements, builder) = exprs.foldLeft((Vector.empty[Stmt], StringBuilder.newBuilder)) {
          case ((stmts, str), e: ComparisonExpr)       => (stmts :+ compExpr(e, allowDirectFilter), str)
          case ((stmts, str), e: InExpr)               => (stmts :+ inExpr(e), str)
          case ((stmts, str), l @ LogicalExpr(And, _)) => (stmts, str.append(fromExpr(l, allowDirectFilter)))
          case ((stmts, str), l: LogicalExpr)          => (stmts, str.append(fromExpr(l, false)))
          case ((stmts, str), NoopExpr)                => (stmts, str)
        }
        fromStmts(And, statements) + builder.mkString
      case LogicalExpr(op, exprs) =>
        val statements = exprs.foldLeft(Vector.empty[Stmt]) {
          case (acc, e: ComparisonExpr) =>
            acc :+ compExpr(e)
          case (acc, e: InExpr) =>
            acc :+ inExpr(e)
          case (acc, _) =>
            // discard nested logical expressions that are not joined by 'And'
            // TODO: need better handling of logical expr
            acc
        }
        fromStmts(op, statements)
    }

    fromExpr(expr, allowDirectFilter = true)
  }

  private implicit val uriTermShow: Show[UriTerm] =
    Show.show(v => s"<${v.value.toString()}>")

  private implicit val litTermShow: Show[LiteralTerm] =
    Show.show(v => v.lexicalForm)

  private implicit val termShow: Show[Term] =
    Show.show {
      case t: UriTerm     => uriTermShow.show(t)
      case t: LiteralTerm => litTermShow.show(t)
    }

  private implicit val comparisonOpShow: Show[ComparisonOp] =
    Show.show {
      case Eq  => "="
      case Ne  => "!="
      case Lt  => "<"
      case Lte => "<="
      case Gt  => ">"
      case Gte => ">="
    }

  private implicit val stmtShow: Show[Stmt] =
    Show.show {
      case Stmt(stmt, Some(filter)) =>
        s"""
           |$stmt
           |FILTER ( $filter )
           |""".stripMargin
      case Stmt(stmt, None) =>
        stmt
    }
}
