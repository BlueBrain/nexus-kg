package ch.epfl.bluebrain.nexus.kg.indexing.query

import cats.MonadError
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.IndexingVocab.SelectTerms
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.Query
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field.Var
import journal.Logger
import org.apache.jena.query.{QuerySolution, ResultSet}
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Bundles queries that can be executed against a SPARQL endpoint.
  *
  * @param client a SPARQL specific client
  * @param F      a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
class SparqlQuery[F[_]](client: SparqlClient[F])(implicit F: MonadError[F, Throwable]) {

  private val log = Logger[this.type]

  /**
    * Performs a full text search query against the SPARQL endpoint producing a collection of results in the ''F[_]''
    * context.
    *
    * @param index the target namespace
    * @param query the SPAQRL query
    * @param Q     the qualifier to map ''uri''s to ''id''s
    * @tparam A the generic type of the response
    * @return a [[QueryResults]] instance wrapped in the abstract ''F[_]'' type
    */
  def apply[A](index: String, query: Query)(implicit Q: ConfiguredQualifier[A]): F[QueryResults[A]] = {

    def scoredQueryResult(rs: ResultSet, sol: QuerySolution): (Option[QueryResult[A]], Option[Long]) = {
      val queryResult = for {
        (subj, score) <- subjectScoreFrom(sol)
        id <- subj.unqualify
      } yield ScoredQueryResult(score, id)
      (queryResult, totalFrom(sol))
    }

    def unscoredQueryResult(rs: ResultSet, sol: QuerySolution): (Option[QueryResult[A]], Option[Long]) = {
      val queryResult = subjectFrom(sol).flatMap(_.unqualify).map(UnscoredQueryResult(_))
      (queryResult, totalFrom(sol))
    }

    def buildQueryResults(scoredResponse: Boolean, listWithTotal: (Vector[QueryResult[A]], Long)) = {
      val (vector, total) = listWithTotal
      if (scoredResponse) QueryResults[A](total, 1F, vector.toList)
      else QueryResults[A](total, vector.toList)
    }

    val serializedQuery = query.pretty
    log.debug(s"Running query: '$serializedQuery'")

    val scoredResponse = query.containsResult(Var(SelectTerms.score))

    client.query(index, serializedQuery).map { rs =>
      val listWithTotal = rs.asScala.foldLeft[(Vector[QueryResult[A]], Long)](Vector.empty -> 0L) {
        case ((queryResults, currentTotal), sol) =>
          val (qr, total) =
            if (scoredResponse) scoredQueryResult(rs, sol)
            else unscoredQueryResult(rs, sol)
          (qr.map(queryResults :+ _).getOrElse(queryResults), total.getOrElse(currentTotal))
      }
      buildQueryResults(scoredResponse, listWithTotal)
    }
  }

  private def subjectScoreFrom(qs: QuerySolution): Option[(String, Float)] = for {
    score   <- scoreFrom(qs)
    subject <- subjectFrom(qs)
  } yield subject -> score

  private def scoreFrom(qs: QuerySolution): Option[Float] =
    Try(qs.get(s"?${SelectTerms.score}").asLiteral().getLexicalForm.toFloat).toOption

  private def totalFrom(qs: QuerySolution): Option[Long] =
    Try(qs.get(s"?${SelectTerms.total}").asLiteral().getLexicalForm.toLong).toOption

  private def subjectFrom(qs: QuerySolution): Option[String] =
    Try(qs.get(s"?${SelectTerms.subject}").asResource().getURI).toOption

}

object SparqlQuery {
  /**
    * Constructs a new ''SparqlQuery[F]'' instance that bundles queries that can be executed against a SPARQL endpoint.
    *
    * @param client a SPARQL specific client
    * @param F      a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F])(implicit F: MonadError[F, Throwable]): SparqlQuery[F] =
    new SparqlQuery[F](client)
}