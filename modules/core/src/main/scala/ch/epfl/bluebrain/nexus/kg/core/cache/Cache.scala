package ch.epfl.bluebrain.nexus.kg.core.cache

import cats.Show
import cats.syntax.show._

/**
  * A cache definition
  *
  * @tparam F     the monadic effect type
  * @tparam Value the generic type of the allowed values to be stored on this cache
  */
trait Cache[F[_], Value] {

  /**
    * Fetches the stored value of the provided ''key''.
    *
    * @param key the key from where to obtain the value
    * @tparam K the generic type of key which is a [[Show]]
    * @return an optional [[Value]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[None]] wrapped within
    *         ''F[_]'' otherwise
    */
  def get[K: Show](key: K): F[Option[Value]] = get(key.show)

  /**
    * Fetches the stored value of the provided ''key''.
    *
    * @param key the key from where to obtain the value
    * @return an optional [[Value]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[None]] wrapped within
    *         ''F[_]'' otherwise
    */
  def get(key: String): F[Option[Value]]

  /**
    * Stores the provided ''value'' on the ''key'' index.
    *
    * @param key   the key location where to store the ''value''
    * @param value the value to store
    * @tparam K the generic type of key which is a [[Show]]
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def put[K: Show](key: K, value: Value): F[Unit] = put(key.show, value)

  /**
    * Stores the provided ''value'' on the ''key'' index.
    *
    * @param key   the key location where to store the ''value''
    * @param value the value to store
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def put(key: String, value: Value): F[Unit]

  /**
    * Removes the value of the provided ''key''.
    *
    * @param key the key from where to obtain the value to be removed
    * @tparam K the generic type of key which is a [[Show]]
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def remove[K: Show](key: K): F[Unit] = remove(key.show)

  /**
    * Removes the value of the provided ''key''.
    *
    * @param key the key from where to obtain the value to be removed
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def remove(key: String): F[Unit]
}
