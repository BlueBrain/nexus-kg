package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field.Var

/**
  * Data type which represents a Blazegraph query with some metadata information.
  *
  * @param selectFields the variables that are going to be retrieved from the query
  * @param serialized   the string representation of the query
  * @param pagination   the optional pagination of the query
  */
final case class Query(private val selectFields: Seq[Var], serialized: String, pagination: Option[Pagination]) {

  override def toString: String = serialized

  /**
    * Checks if the provided variable is going to be returned in the query response.
    *
    * @param variable the provided variable
    * @return true if the variable is going to be returned in the response
    *         false otherwise.
    */
  def containsResult(variable: Var): Boolean = selectFields.contains(variable)

  /**
    * Indent the query response.
    *
    * @return an indented string representation of the ''serialized'' query
    */
  def pretty: String = {
    def nextIndent(line: String, indent: Int) =
      if (line.startsWith("{") || line.endsWith("{")) indent + 1
      else if (line.endsWith("}")) indent - 1
      else indent

    val (builder, _) = serialized.split('\n').foldLeft(new StringBuilder -> 0) {
      case ((sb, indent), line) =>
        val trimmed = line.trim
        if (trimmed.isEmpty)
          sb -> indent
        else if (trimmed.startsWith("}"))
          (sb ++= "\n" ++= "\t" * (indent - 1) ++= trimmed) -> (indent - 1)
        else
          (sb ++= "\n" ++= "\t" * indent ++= trimmed) -> nextIndent(trimmed, indent)
    }
    builder.toString.trim
  }
}
