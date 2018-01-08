package ch.epfl.bluebrain.nexus.kg.core.queries

/**
  * Reference data type to a specific query revision.
  *
  * @param id  the unique identifier for the query
  * @param rev a revision identifier for the query
  */
final case class QueryRef(id: QueryId, rev: Long)
