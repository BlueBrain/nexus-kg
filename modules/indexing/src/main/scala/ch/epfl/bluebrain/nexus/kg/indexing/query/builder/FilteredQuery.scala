package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import java.util.concurrent.atomic.AtomicInteger

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, InExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, TermCollection, UriTerm}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Expr, Filter, Term}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination

/**
  * Describes paginated queries based on filters.
  */
object FilteredQuery {

  /**
    * Constructs a SPARQL query based on the provided filter and pagination settings that also computes the total
    * number of results.  The filter is applied on the subjects.
    *
    * @param filter     the filter on which the query is based on
    * @param pagination the pagination settings for the generated query
    * @return a SPARQL query based on the provided filter and pagination settings
    */
  def apply(filter: Filter, pagination: Pagination): String = {
    s"""
      |SELECT ?total ?s
      |WITH {
      |  SELECT ?s
      |  WHERE {
      |${buildWhere(filter.expr, "?s", "var")}
      |  }
      |} AS %resultSet
      |WHERE {
      |  {
      |    SELECT (COUNT(DISTINCT ?s) AS ?total)
      |    WHERE { INCLUDE %resultSet }
      |  }
      |  UNION
      |  {
      |    SELECT *
      |    WHERE { INCLUDE %resultSet }
      |    LIMIT ${pagination.size}
      |    OFFSET ${pagination.from}
      |  }
      |}""".stripMargin
  }

  /**
    * Constructs a SPARQL query based on the provided filters and pagination settings that also computes the total
    * number of results.
    *
    * @param thisFilter   the filter to be applied to select the subjects
    * @param targetFilter the filter to be applied to filter the objects
    * @param pagination   the pagination settings for the generated query
    */
  def outgoing(thisFilter: Filter, targetFilter: Filter, pagination: Pagination): String = {
    s"""
       |SELECT ?total ?s
       |WITH {
       |  SELECT ?s
       |  WHERE {
       |
       |?ss ?p ?s .
       |${buildWhere(thisFilter.expr, "?ss", "this")}
       |${buildWhere(targetFilter.expr, "?s", "var")}
       |  }
       |} AS %resultSet
       |WHERE {
       |  {
       |    SELECT (COUNT(DISTINCT ?s) AS ?total)
       |    WHERE { INCLUDE %resultSet }
       |  }
       |  UNION
       |  {
       |    SELECT *
       |    WHERE { INCLUDE %resultSet }
       |    LIMIT ${pagination.size}
       |    OFFSET ${pagination.from}
       |  }
       |}""".stripMargin
  }

  /**
    * Constructs a SPARQL query based on the provided filters and pagination settings that also computes the total
    * number of results.
    *
    * @param thisFilter   the filter to be applied to select the objects
    * @param sourceFilter the filter to be applied to filter the subjects
    * @param pagination   the pagination settings for the generated query
    */
  def incoming(thisFilter: Filter, sourceFilter: Filter, pagination: Pagination): String = {
    s"""
       |SELECT ?total ?s
       |WITH {
       |  SELECT ?s
       |  WHERE {
       |
       |?s ?p ?ss .
       |${buildWhere(thisFilter.expr, "?ss", "this")}
       |${buildWhere(sourceFilter.expr, "?s", "var")}
       |  }
       |} AS %resultSet
       |WHERE {
       |  {
       |    SELECT (COUNT(DISTINCT ?s) AS ?total)
       |    WHERE { INCLUDE %resultSet }
       |  }
       |  UNION
       |  {
       |    SELECT *
       |    WHERE { INCLUDE %resultSet }
       |    LIMIT ${pagination.size}
       |    OFFSET ${pagination.from}
       |  }
       |}""".stripMargin
  }

  private final case class Stmt(stmt: String, filter: String, variable: String)

  private def buildWhere(expr: Expr, subjectVar: String, varPrefix: String): String = {
    val atomicIdx = new AtomicInteger(0)
    def nextIdx(): Int =
      atomicIdx.incrementAndGet()

    // ?s :p ?var
    // FILTER (?var IN (term1, term2))
    def inExpr(expr: InExpr): Stmt = {
      val idx = nextIdx()
      val variable = s"?${varPrefix}_$idx"
      val InExpr(path, TermCollection(terms)) = expr
      val stmt = s"$subjectVar ${path.show} $variable ."
      val filter = terms.map(_.show).mkString(s"$variable IN (", ", ", ")")
      Stmt(stmt, filter, variable)
    }

    // ?s :p ?var .
    // FILTER ( ?var op term )
    def compExpr(expr: ComparisonExpr): Stmt = {
      val idx = nextIdx()
      val variable = s"?${varPrefix}_$idx"
      val ComparisonExpr(op, path, term) = expr
      Stmt(s"$subjectVar ${path.show} $variable .", s"$variable ${op.show} ${term.show}", variable)
    }

    def fromStmts(op: LogicalOp, stmts: Vector[Stmt]): String = op match {
      case And =>
        val select = stmts.map(_.stmt).mkString("\n")
        val filter = stmts.map(_.filter).mkString(" && ")
        s"""
           |$select
           |FILTER ( $filter )
           |""".stripMargin
      case Or =>
        val select = stmts.map(v => s"OPTIONAL { ${v.stmt} }").mkString("\n")
        val filter = stmts.map(_.filter).mkString(" || ")
        s"""
           |$select
           |FILTER ( $filter )
           |""".stripMargin
      case Not =>
        val select = stmts.map(_.stmt).mkString("\n")
        val filter = stmts.map(_.filter).mkString(" || ")
        s"""
           |$select
           |FILTER NOT EXISTS {
           |$select
           |FILTER ( $filter )
           |}
           |""".stripMargin
      case Xor =>
        val optSelect = stmts.map(v => s"OPTIONAL { ${v.stmt} }").mkString("\n")
        val filter = stmts.map(_.filter).mkString(" || ")
        val select = stmts.map(_.stmt).mkString("\n")
        s"""
           |$optSelect
           |FILTER ( $filter )
           |FILTER NOT EXISTS {
           |$select
           |FILTER ( $filter )
           |}
           |""".stripMargin

    }

    def fromExpr(expr: Expr): String = expr match {
      case NoopExpr                => s"$subjectVar ?p ?o ."
      case e: ComparisonExpr       => compExpr(e).show
      case e: InExpr               => inExpr(e).show
      case LogicalExpr(And, exprs) =>
        val (statements, builder) = exprs.foldLeft((Vector.empty[Stmt], StringBuilder.newBuilder)) {
          case ((stmts, str), e: ComparisonExpr) => (stmts :+ compExpr(e), str)
          case ((stmts, str), e: InExpr)         => (stmts :+ inExpr(e), str)
          case ((stmts, str), l: LogicalExpr)    => (stmts, str.append(fromExpr(l)))
          case ((stmts, str), NoopExpr)          => (stmts, str)
        }
        fromStmts(And, statements) + builder.mkString
      case LogicalExpr(op, exprs)  =>
        val statements = exprs.foldLeft(Vector.empty[Stmt]) {
          case (acc, e: ComparisonExpr) =>
            acc :+ compExpr(e)
          case (acc, e: InExpr)         =>
            acc :+ inExpr(e)
          case (acc, _)                 =>
            // discard nested logical expressions that are not joined by 'And'
            // TODO: need better handling of logical expr
            acc
        }
        fromStmts(op, statements)
    }

    fromExpr(expr)
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
      case Stmt(stmt, filter, _) =>
        s"""
           |$stmt
           |FILTER ( $filter )
           |""".stripMargin
    }
}