package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.StorageConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Rejection, ResId, ResourceV}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.{Fetch, Save}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{FetchFile, SaveFile}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._

/**
  * Contract for different types of storage back-end.
  */
sealed trait Storage { self =>

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
    * @return the deprecation state of the store
    */
  def deprecated: Boolean

  /**
    * @return ''true'' if this store is the project's default backend, ''false'' otherwise
    */
  def default: Boolean

  /**
    *
    * @return the digest algorithm, e.g. "SHA-256"
    */
  def algorithm: String

  /**
    * @return a generated name that uniquely identifies the store and its current revision
    */
  def name: String = s"${ref.id}_${id.asString}_$rev"

  /**
    * Provides a [[SaveFile]] instance.
    *
    */
  def save[F[_], In](implicit save: Save[F, In]): SaveFile[F, In] = save(self)

  /**
    * Provides a [[FetchFile]] instance.
    */
  def fetch[F[_], Out](implicit fetch: Fetch[Out]): FetchFile[Out] = fetch(self)

}

object Storage {

  /**
    * A disk storage
    *
    * @param ref        a reference to the project that the store belongs to
    * @param id         the user facing store id
    * @param rev        the store revision
    * @param deprecated the deprecation state of the store
    * @param default    ''true'' if this store is the project's default backend, ''false'' otherwise
    * @param algorithm  the digest algorithm, e.g. "SHA-256"
    * @param volume     the volume this storage is going to use to save files
    */
  final case class DiskStorage(ref: ProjectRef,
                               id: AbsoluteIri,
                               rev: Long,
                               deprecated: Boolean,
                               default: Boolean,
                               algorithm: String,
                               volume: Path)
      extends Storage

  object DiskStorage {

    /**
      * Default [[DiskStorage]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef)(implicit config: StorageConfig): DiskStorage =
      DiskStorage(ref,
                  nxv.defaultStorage,
                  1L,
                  deprecated = false,
                  default = true,
                  config.disk.digestAlgorithm,
                  config.disk.volume.resolve(ref.id.toString))
  }

  /**
    * Amazon Cloud Storage Service
    *
    * @param ref        a reference to the project that the store belongs to
    * @param id         the user facing store id
    * @param rev        the store revision
    * @param deprecated the deprecation state of the store
    * @param default    ''true'' if this store is the project's default backend, ''false'' otherwise
    * @param algorithm  the digest algorithm, e.g. "SHA-256"
    */
  final case class S3Storage(ref: ProjectRef,
                             id: AbsoluteIri,
                             rev: Long,
                             deprecated: Boolean,
                             default: Boolean,
                             algorithm: String)
      extends Storage

  /**
    * Attempts to transform the resource into a [[Storage]].
    *
    * @param res a materialized resource
    * @return Right(storage) if the resource is compatible with a Storage, Left(rejection) otherwise
    */
  final def apply(res: ResourceV)(implicit config: StorageConfig): Either[Rejection, Storage] = {
    val c = res.value.graph.cursor()

    def diskStorage(): Either[Rejection, Storage] =
      for {
        default <- c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref)
        volume  <- c.downField(nxv.volume).focus.as[String].map(Paths.get(_)).toRejectionOnLeft(res.id.ref)
      } yield
        DiskStorage(res.id.parent, res.id.value, res.rev, res.deprecated, default, config.disk.digestAlgorithm, volume)

    def s3Storage(): Either[Rejection, Storage] =
      c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref).map { default =>
        S3Storage(res.id.parent, res.id.value, res.rev, res.deprecated, default, config.amazon.digestAlgorithm)
      }

    if (Set(nxv.Storage.value, nxv.DiskStorage.value).subsetOf(res.types)) diskStorage()
    else if (Set(nxv.Storage.value, nxv.Alpha.value, nxv.S3Storage.value).subsetOf(res.types)) s3Storage()
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
  }

  trait FetchFile[Out] {

    /**
      * Fetches the file associated to the provided ''fileMeta''.
      *
      * @param fileMeta the file metadata
      */
    def apply(fileMeta: FileAttributes): Out
  }

  trait SaveFile[F[_], In] {

    /**
      * Stores the provided stream source.
      *
      * @param id       the id of the resource
      * @param fileDesc the file descriptor to be stored
      * @param source   the source
      * @return [[FileAttributes]] wrapped in the abstract ''F[_]'' type if successful,
      *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
      */
    def apply(id: ResId, fileDesc: FileDescription, source: In): F[FileAttributes]
  }

  object StorageOperations {

    /**
      * Provides a selected storage with [[FetchFile]] operation
      *
      * @tparam Out the output type
      */
    trait Fetch[Out] {
      def apply(storage: Storage): FetchFile[Out]
    }
    object Fetch {
      implicit final def apply: Fetch[AkkaSource] = {
        case value: DiskStorage => new DiskStorageOperations.Fetch(value)
        case _: S3Storage       => ??? //TODO
      }
    }

    /**
      * Provides a selected storage with [[SaveFile]] operation
      *
      * @tparam F   the effect type
      * @tparam In  the input type
      */
    trait Save[F[_], In] {
      def apply(storage: Storage): SaveFile[F, In]
    }

    object Save {
      implicit final def apply[F[_]: Effect](implicit as: ActorSystem): Save[F, AkkaSource] = {
        case value: DiskStorage => new DiskStorageOperations.Save(value)
        case _: S3Storage       => ??? //TODO
      }
    }
  }
}
