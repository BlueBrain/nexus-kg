package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.{Path, Paths}
import java.time.Instant

import ch.epfl.bluebrain.nexus.kg.config.AppConfig.FileConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Rejection, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._

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

    /**
      * Default [[FileConfig]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef)(implicit config: FileConfig): Storage =
      FileStorage(ref,
                  nxv.defaultStorage,
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

  /**
    * Attempts to transform the resource into a [[Storage]].
    *
    * @param res a materialized resource
    * @return Right(storage) if the resource is compatible with a Storage, Left(rejection) otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, Storage] = {
    val c = res.value.graph.cursor()

    def fileStorage(): Either[Rejection, Storage] =
      for {
        default   <- c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref)
        algorithm <- c.downField(nxv.algorithm).focus.as[String].toRejectionOnLeft(res.id.ref)
        volume    <- c.downField(nxv.volume).focus.as[String].map(Paths.get(_)).toRejectionOnLeft(res.id.ref)
      } yield
        (FileStorage(res.id.parent, res.id.value, res.rev, res.updated, res.deprecated, default, algorithm, volume))

    def s3Storage(): Either[Rejection, Storage] =
      for {
        default   <- c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref)
        algorithm <- c.downField(nxv.algorithm).focus.as[String].toRejectionOnLeft(res.id.ref)
      } yield (S3Storage(res.id.parent, res.id.value, res.rev, res.updated, res.deprecated, default, algorithm))

    if (Set(nxv.Storage.value, nxv.Alpha.value, nxv.FileStorage.value).subsetOf(res.types)) fileStorage()
    else if (Set(nxv.Storage.value, nxv.Alpha.value, nxv.S3Storage.value).subsetOf(res.types)) s3Storage()
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
  }

}
