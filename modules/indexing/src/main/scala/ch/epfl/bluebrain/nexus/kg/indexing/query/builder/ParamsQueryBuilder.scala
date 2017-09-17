package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field.{Expr, Var}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.{Triple, TripleContent}

/**
  * Creates a Blazegraph query from the provided [[QueryParams]]
  *
  * @param params the provided parameters to build a Blazegraph query
  */
class ParamsQueryBuilder(override val params: QueryParams) extends QueryBuilder {

  /**
    * Constructs a [[Query]] serializing the different steps found on ''params''
    *
    * @return an instance of a [[Query]]
    */
  final def apply(): Query = {

    def selectVars(): Seq[Var] =
      params.select.collect {
        case field: Var              => field
        case Expr(_, Some(variable)) => Var(variable)
      }

    val serialized = List(serializePrefixes, serializeSelects, serializeWith, serializeWheres, serializeGroup, serializeOrder, serializePagination)
      .foldLeft(new StringBuilder)((serialized, block) => {
        block.map(serialized ++= _ ++= "\n").getOrElse(serialized)
      }).toString().trim
    Query(selectVars(), serialized, params.pagination)
  }

  private def serializePrefixes: Option[String] =
    (params.prefix, "\n")

  private def serializeSelects: Option[String] = {
    val prefix = s"SELECT ${if (params.distinct) "DISTINCT " else ""}"
    (params.select, " ").map(res => s"$prefix$res")
  }

  private def serializeWith: Option[String] =
    (params.withs, (tuple: (QueryParams, String)) => s"WITH {\n${ParamsQueryBuilder(tuple._1).build().toString}\n} AS %${tuple._2}", "\n")

  private def serializeWheres: Option[String] = {
    val triples = params.where.reverseMap(_.show).mkString("\n").trim
    val filterTriples = params.filter.map(result => s"$triples\nFILTER ( $result )".trim).getOrElse(triples)
    val filterTriplesInclude = serializeIncludes.map(result => s"$filterTriples\n$result").getOrElse(filterTriples)
    val mainBlock = serializeOptional.map(result => s"$filterTriplesInclude\n$result").getOrElse(filterTriplesInclude)

    val subQuery = serializeSubQuery
    val union = serializeUnion

    (for (s <- subQuery; u <- union) yield s"$s\n$u\n$mainBlock")
      .orElse(subQuery.map(_ + "\n" + mainBlock))
      .orElse(union.map(result => s"{ $mainBlock\n}\n$result"))
      .orElse(Some(mainBlock)).map(result => s"WHERE \n{\n$result\n}")
  }

  private def serializeOptional: Option[String] =
    (params.optional.reverse, (values: Triple[TripleContent]) => s"OPTIONAL {\n${values.show}\n}", "\n")

  private def serializeGroup: Option[String] =
    (params.group, " ").map(res => s"GROUP BY $res")

  private def serializeOrder: Option[String] =
    (params.order.reverse, " ").map(res => s"ORDER BY $res")

  private def serializePagination: Option[String] =
    params.pagination
      .map(p => (s"LIMIT ${p.size}", p.from))
      .map {
        case (result, from) if from > 0 => s"$result\nOFFSET $from"
        case (result, _)                => result
      }

  private def serializeSubQuery: Option[String] =
    (params.subQueries.reverse, (params: QueryParams) => s"{ ${ParamsQueryBuilder(params).build().toString}\n}", "\n")

  private def serializeUnion: Option[String] =
    (params.unions.reverse, (params: QueryParams) => s"UNION \n{\n${ParamsQueryBuilder(params).build().toString}\n}", "")

  private def serializeIncludes: Option[String] =
    (params.includes, (name: String) => s"INCLUDE %$name", "\n")

  private implicit def optional[A](tuple: (Seq[A], String))(implicit S: Show[A]): Option[String] = {
    val (seq, delim) = tuple
    Option(seq.map(_.show)).filter(_.nonEmpty).map(_.mkString(delim))
  }

  private implicit def optional[A](tuple: (Seq[A], A => String, String)): Option[String] = {
    val (seq, f, delim) = tuple
    Option(seq.map(f(_))).filter(_.nonEmpty).map(_.mkString(delim))
  }

}

object ParamsQueryBuilder {
  /**
    * Constructs a query builder form the [[QueryParams]]
    *
    * @param params the provided parameters to build a Blazegraph query
    * @return an instance of [[ParamsQueryBuilder]]
    */
  final def apply(params: QueryParams): ParamsQueryBuilder = new ParamsQueryBuilder(params)
}
