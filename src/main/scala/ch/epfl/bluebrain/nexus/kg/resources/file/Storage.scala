package ch.epfl.bluebrain.nexus.kg.resources.file

import java.time.Instant
import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Contract for different types of storage back-end.
  */
trait Storage {

  /**
    * @return a reference to the project that the store belongs to
    */
  def ref: ProjectRef

  /**
    * @return the user facing store id
    */
  def id: AbsoluteIri

  /**
    * @return the underlying uuid generated for this store
    */
  def uuid: UUID

  /**
    * @return the store revision
    */
  def rev: Long

  /**
    * @return the instant when this store was updated
    */
  def instant: Instant

  /**
    * @return the deprecation state of the store
    */
  def deprecated: Boolean

  /**
    * @return ''true'' if this store is the project's default backend
    */
  def default: Boolean

  /**
    *
    * @return the digest algorithm, e.g. "SHA-256"
    */
  def digestAlgorithm: String

  /**
    * @return a generated name that uniquely identifies the store and its current revision
    */
  def name: String = s"${ref.id}_${uuid}_$rev"

}
