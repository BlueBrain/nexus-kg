package ch.epfl.bluebrain.nexus.kg.core.queries

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.cache.Cache
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryRejection._
import journal.Logger

import scala.util.matching.Regex

/**
  * Bundles operations that can be performed against organizations using the underlying persistence abstraction.
  *
  * @param cache the cache definition
  * @param F   a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
final class Queries[F[_]](cache: Cache[F, Query])(implicit F: MonadError[F, Throwable]) {

  private val logger = Logger[this.type]

  /**
    * Creates a new query instance by generating a unique identifier for it.
    *
    * @param path the path where the query will take effect
    * @param value the value of the query
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.queries.QueryId]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def create(path: Path, value: QueryPayload): F[QueryId] = {
    val query = Query(path, value)
    val check = for {
      _      <- validateId(query.id)
      exists <- fetch(query.id).map(!_.isDefined)
    } yield (exists)
    check flatMap {
      case true =>
        cache.put(query.id, query).map { _ =>
          logger.debug(s"Creation for query with '${query.id}' succeed with payload '$value'")
          query.id
        }
      case false =>
        logger.debug(s"There is already a query for the id '${query.id}'. Attempting to create it with another id.")
        create(path, value)
    }
  }

  /**
    * Updates the selected query with a new json value.
    *
    * @param id    the unique identifier of the query
    * @param value the value of the query
    * @return an [[ch.epfl.bluebrain.nexus.kg.core.queries.QueryId]] instance wrapped in the abstract ''F[_]'' type
    *         if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within ''F[_]'' otherwise
    */
  def update(id: QueryId, value: QueryPayload): F[QueryId] =
    fetch(id) flatMap {
      case Some(q) =>
        logger.debug(s"Update for query with '$id' succeed with payload '$value'")
        cache.put(id, q.copy(value = value)).map(_ => id)
      case None =>
        logger.error(s"Update for query with '$id' failed, query not found on cache")
        F.raiseError(CommandRejected(QueryDoesNotExist))
    }

  /**
    * Queries the system for a query instance matching the argument ''id''.
    *
    * @param id the unique identifier of the query
    * @return an optional [[ch.epfl.bluebrain.nexus.kg.core.queries.Query]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[ch.epfl.bluebrain.nexus.kg.core.Fault]] wrapped within
    *         ''F[_]'' otherwise
    */
  def fetch(id: QueryId): F[Option[Query]] =
    cache.get(id)

  private val regex: Regex =
    """[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}""".r

  private def validateId(id: QueryId): F[Unit] = {
    F.pure {
      logger.debug(s"Validating id '$id'")
      id.id
    } flatMap {
      case regex() =>
        logger.debug(s"Id validation for '$id' succeeded")
        F.pure(())
      case _ =>
        logger.debug(s"Id validation for '$id' failed, did not match regex '$regex'")
        F.raiseError(CommandRejected(InvalidQueryId(id.id)))
    }
  }

}
object Queries {

  /**
    * Constructs a new ''Queries'' instance that bundles operations that can be performed against queries
    * using the underlying persistence abstraction.
    *
    * @param cache the cache definition
    * @param F   a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](cache: Cache[F, Query])(implicit F: MonadError[F, Throwable]): Queries[F] =
    new Queries[F](cache)
}
