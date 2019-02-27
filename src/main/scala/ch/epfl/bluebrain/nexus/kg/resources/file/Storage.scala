package ch.epfl.bluebrain.nexus.kg.resources.file

import java.time.Instant
import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResId}
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}
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
    * @return a generated name that uniquely identifies the store and its current revision
    */
  def name: String = s"${ref.id}_${uuid}_$rev"

  /**
    * Stores the provided stream source using an implicitly available [[StorageOperations]] instance.
    *
    * @param id       the id of the resource
    * @param fileDesc the file descriptor to be stored
    * @param source   the source
    * @return [[FileAttributes]] wrapped in the abstract ''F[_]'' type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save[F[_], ST, In](id: ResId, fileDesc: FileDescription, source: In)(
      implicit storage: StorageOperations[ST, In, _]): F[FileAttributes] =
    storage.save[F](id, fileDesc, source)

  /**
    * Fetches the file associated to the provided ''fileMeta''  sing an implicitly available [[StorageOperations]] instance.
    *
    * @param fileMeta the file metadata
    */
  def fetch[ST, Out](fileMeta: FileAttributes)(implicit storage: StorageOperations[ST, _, Out]): Out =
    storage.fetch(fileMeta)

}
