package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field.{Expr, Var}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.TotalCount

/**
  * Creates a SPARQL query from the provided [[QueryParams]]
  *
  * @param params the provided parameters to build a SPARQL query
  */
class ParamsQueryBuilder(override val params: QueryParams) extends QueryBuilder {

  /**
    * Constructs a [[Query]] serializing the different steps found on ''params''
    *
    * @return an instance of a [[Query]]
    */
  def apply(): Query = params.total.map(applyWithTotal).getOrElse(applyDefault)

  private def applyDefault(): Query = {
    val serialized = List(serializePrefixes, serializeSelects, serializeWheres, serializeGroup, serializePagination)
      .foldLeft(new StringBuilder)((serialized, block) => {
        block.map(serialized ++= _ ++= "\n").getOrElse(serialized)
      }).toString().trim
    Query(selectVars(), serialized, params.pagination)
  }

  private def applyWithTotal(total: TotalCount): Query = {

    val queryParams = params.copy(total = None, prefix = Seq.empty)

    val countQueryParams = params.copy(group = Seq.empty, pagination = None, total = None, prefix = Seq.empty,
      select = Seq(Expr(total.expr, total.variable)))

    val finalParams = QueryParams(select = Var("total") +: params.select,
      subQueries = Seq(countQueryParams),
      unions = Seq(queryParams),
      prefix = params.prefix)

    ParamsQueryBuilder(finalParams).build
  }

  private def serializePrefixes: Option[String] =
    (params.prefix, "\n")

  private def serializeSelects: Option[String] = {
    val prefix = s"SELECT ${if (params.distinct) "DISTINCT " else ""}"
    (params.select, " ").map(res => s"$prefix$res")
  }

  private def serializeWheres: Option[String] = {
    val triples = whereTriples.getOrElse("")
    val filterTriples = params.filter.map(result => s"$triples\nFILTER ( $result )".trim).getOrElse(triples)
    val subQuery = serializeSubQuery
    val union = serializeUnion

    (for (s <- subQuery; u <- union) yield (s"$s\n$u\n$filterTriples"))
      .orElse(subQuery.map(_ + "\n" + filterTriples))
      .orElse(union.map(result => s"{ $filterTriples\n}\n$result"))
      .orElse(Some(filterTriples)).map(result => s"WHERE \n{\n$result\n}")
  }


  private def serializeGroup: Option[String] =
    ((params.group, " ")).map(res => s"GROUP BY $res")

  private def serializePagination: Option[String] =
    params.pagination
      .map(p => (s"LIMIT ${p.size}", p.from))
      .map {
        case (result, from) if (from > 0) => s"$result\nOFFSET $from"
        case (result, _)                  => result
      }

  private def whereTriples: Option[String] =
    (params.where.reverse, "\n")

  private def serializeSubQuery: Option[String] =
    (params.subQueries.reverse, (params: QueryParams) => s"{ ${ParamsQueryBuilder(params).build.toString}\n}", "\n")

  private def serializeUnion: Option[String] =
    (params.unions.reverse, (params: QueryParams) => s"UNION \n{\n${ParamsQueryBuilder(params).build.toString}\n}", "")


  private implicit def optional[A](tuple: (Seq[A], String))(implicit S: Show[A]): Option[String] = {
    val (seq, delim) = tuple
    Option(seq.map(_.show)).filter(_.nonEmpty).map(_.mkString(delim))
  }

  private implicit def optional[A](tuple: (Seq[A], A => String, String)): Option[String] = {
    val (seq, f, delim) = tuple
    Option(seq.map(f(_))).filter(_.nonEmpty).map(_.mkString(delim))
  }

  private def selectVars(): Seq[Var] = params.select.map(v => Var(v.variable))
}

object ParamsQueryBuilder {
  /**
    * Constructs a query builder form the [[QueryParams]]
    *
    * @param params the provided parameters to build a SPARQL query
    * @return an instance of [[ParamsQueryBuilder]]
    */
  final def apply(params: QueryParams): ParamsQueryBuilder = new ParamsQueryBuilder(params)
}
