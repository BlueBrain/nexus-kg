package ch.epfl.bluebrain.nexus.kg.resources.file

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResId}
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Contract for different types of storage back-end.
  */
trait Store[F[_], In, Out] {

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
    * @return the deprecation state of the store
    */
  def deprecated: Boolean

  /**
    * @return ''true'' if this store is the project's default backend
    */
  def default: Boolean

  /**
    * @return a generated name that uniquely identifies the store and its current revision
    */
  def name: String = s"${ref.id}_${uuid}_$rev"

  /**
    * Stores the provided stream source.
    *
    * @param id       the id of the resource
    * @param fileDesc the file descriptor to be stored
    * @param source   the source
    * @return [[FileAttributes]] wrapped in the abstract ''F[_]'' type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save(id: ResId, fileDesc: FileDescription, source: In): F[FileAttributes]

  /**
    * Fetches the file associated to the provided ''fileMeta''.
    *
    * @param fileMeta the file metadata
    */
  def fetch(fileMeta: FileAttributes): Out

}
