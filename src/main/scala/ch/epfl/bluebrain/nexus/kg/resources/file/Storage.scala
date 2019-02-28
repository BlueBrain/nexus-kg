package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.Path
import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.AppConfig.FileConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * Contract for different types of storage back-end.
  */
sealed trait Storage {

  /**
    * @return a reference to the project that the store belongs to
    */
  def ref: ProjectRef

  /**
    * @return the user facing store id
    */
  def id: AbsoluteIri

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
  def name: String = s"${ref.id}_${id.asString}_$rev"

}

object Storage {
  final case class FileStorage(ref: ProjectRef,
                               id: AbsoluteIri,
                               rev: Long,
                               instant: Instant,
                               deprecated: Boolean,
                               default: Boolean,
                               digestAlgorithm: String,
                               volume: Path)
      extends Storage
  object FileStorage {
    def default(ref: ProjectRef)(implicit config: FileConfig): Storage =
      FileStorage(ref,
                  nxv.defaultFileStorage,
                  1L,
                  Instant.EPOCH,
                  deprecated = false,
                  default = true,
                  config.digestAlgorithm,
                  config.volume.resolve(ref.id.toString))
  }

  final case class S3Storage(ref: ProjectRef,
                             id: AbsoluteIri,
                             rev: Long,
                             instant: Instant,
                             deprecated: Boolean,
                             default: Boolean,
                             digestAlgorithm: String)
      extends Storage

}
