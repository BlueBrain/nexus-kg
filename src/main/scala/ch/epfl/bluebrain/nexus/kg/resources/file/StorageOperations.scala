package ch.epfl.bluebrain.nexus.kg.resources.file

import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}

/**
  * Contract for storage operations.
  *
  * @tparam StoreType the store type
  * @tparam In        the input type
  * @tparam Out       the output type
  */
trait StorageOperations[StoreType, In, Out] {

  /**
    * Stores the provided stream source.
    *
    * @param id       the id of the resource
    * @param fileDesc the file descriptor to be stored
    * @param source   the source
    * @return [[FileAttributes]] wrapped in the abstract ''F[_]'' type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save[F[_]](id: ResId, fileDesc: FileDescription, source: In): F[FileAttributes]

  /**
    * Fetches the file associated to the provided ''fileMeta''.
    *
    * @param fileMeta the file metadata
    */
  def fetch(fileMeta: FileAttributes): Out

}
