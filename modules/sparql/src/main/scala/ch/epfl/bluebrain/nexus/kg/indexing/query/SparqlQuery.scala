package ch.epfl.bluebrain.nexus.kg.indexing.query

import cats.MonadError
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms
import journal.Logger
import org.apache.jena.query.QuerySolution

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Bundles queries that can be executed against a SPAQRL endpoint.
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
    * @param index  the target namespace
    * @param query  the serialized SPAQRL/Blazegraph query
    * @param scored whether the query expects scored results
    * @param Q      the qualifier to map ''uri''s to ''id''s
    * @tparam A the generic type of the response
    * @return a [[QueryResults]] instance wrapped in the abstract ''F[_]'' type
    */
  def apply[A](index: String, query: String, scored: Boolean)(
      implicit Q: ConfiguredQualifier[A]): F[QueryResults[A]] = {
    def scoredQueryResult(sol: QuerySolution): (Option[QueryResult[A]], Option[Long], Option[Float]) = {
      val queryResult = for {
        (subj, score) <- subjectScoreFrom(sol)
        id            <- subj.unqualify
      } yield ScoredQueryResult(score, id)
      (queryResult, totalFrom(sol), maxScoreFrom(sol))
    }

    def unscoredQueryResult(sol: QuerySolution): (Option[QueryResult[A]], Option[Long], Option[Float]) = {
      val queryResult = subjectFrom(sol).flatMap(_.unqualify).map(UnscoredQueryResult(_))
      (queryResult, totalFrom(sol), None)
    }

    def buildQueryResults(scoredResponse: Boolean, listWithTotal: (Vector[QueryResult[A]], Long, Float)) = {
      val (vector, total, maxScore) = listWithTotal
      if (scoredResponse) QueryResults[A](total, maxScore, vector.toList)
      else QueryResults[A](total, vector.toList)
    }

    log.debug(s"Running query: '$query'")

    client.query(index, query).map { rs =>
      val listWithTotal = rs.asScala.foldLeft[(Vector[QueryResult[A]], Long, Float)]((Vector.empty, 0L, 0F)) {
        case ((queryResults, currentTotal, currentMaxScore), sol) =>
          val (qr, total, maxScore) =
            if (scored) scoredQueryResult(sol)
            else unscoredQueryResult(sol)
          (qr.map(queryResults :+ _).getOrElse(queryResults),
           total.getOrElse(currentTotal),
           maxScore.getOrElse(currentMaxScore))
      }
      buildQueryResults(scored, listWithTotal)
    }
  }

  private def subjectScoreFrom(qs: QuerySolution): Option[(String, Float)] =
    for {
      score   <- scoreFrom(qs)
      subject <- subjectFrom(qs)
    } yield subject -> score

  private def scoreFrom(qs: QuerySolution): Option[Float] =
    Try(qs.get(s"?${SelectTerms.score}").asLiteral().getLexicalForm.toFloat).toOption

  private def totalFrom(qs: QuerySolution): Option[Long] =
    Try(qs.get(s"?${SelectTerms.total}").asLiteral().getLexicalForm.toLong).toOption

  private def subjectFrom(qs: QuerySolution): Option[String] =
    Try(qs.get(s"?${SelectTerms.subject}").asResource().getURI).toOption

  private def maxScoreFrom(qs: QuerySolution): Option[Float] =
    Try(qs.get(s"?${SelectTerms.maxScore}").asLiteral().getLexicalForm.toFloat).toOption

}

object SparqlQuery {

  /**
    * Constructs a new ''SparqlQuery[F]'' instance that bundles queries that can be executed
    * against a Blazegraph endpoint.
    *
    * @param client a SPARQL specific client
    * @param F      a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F])(implicit F: MonadError[F, Throwable]): SparqlQuery[F] =
    new SparqlQuery[F](client)
}
