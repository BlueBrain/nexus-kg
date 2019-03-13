package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.s3
import akka.stream.alpakka.s3.{ApiVersion, MemoryBufferType}
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.StorageConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Rejection, ResId, ResourceV}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{FetchFile, SaveFile, VerifyStorage}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder.stringEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.syntax._
import com.amazonaws.auth._
import com.amazonaws.regions.{AwsRegionProvider, DefaultAwsRegionProviderChain}

import scala.collection.JavaConverters._
import scala.util.Try

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
    * @return the permission required in order to download a file from this storage
    */
  def readPermission: Permission

  /**
    * @return the permission required in order to upload a file to this storage
    */
  def writePermission: Permission

  /**
    * Provides a [[SaveFile]] instance.
    *
    */
  def save[F[_], In](implicit save: Save[F, In]): SaveFile[F, In] = save(self)

  /**
    * Provides a [[FetchFile]] instance.
    */
  def fetch[F[_], Out](implicit fetch: Fetch[Out]): FetchFile[Out] = fetch(self)

  /**
    * Provides a [[VerifyStorage]] instance.
    */
  def isValid[F[_]](implicit verify: Verify[F]): VerifyStorage[F] = verify(self)

}

object Storage {

  /**
    * A disk storage
    *
    * @param ref             a reference to the project that the store belongs to
    * @param id              the user facing store id
    * @param rev             the store revision
    * @param deprecated      the deprecation state of the store
    * @param default         ''true'' if this store is the project's default backend, ''false'' otherwise
    * @param algorithm       the digest algorithm, e.g. "SHA-256"
    * @param volume          the volume this storage is going to use to save files
    * @param readPermission  the permission required in order to download a file from this storage
    * @param writePermission the permission required in order to upload a file to this storage
    */
  final case class DiskStorage(ref: ProjectRef,
                               id: AbsoluteIri,
                               rev: Long,
                               deprecated: Boolean,
                               default: Boolean,
                               algorithm: String,
                               volume: Path,
                               readPermission: Permission,
                               writePermission: Permission)
      extends Storage

  object DiskStorage {

    /**
      * Default [[DiskStorage]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef)(implicit config: StorageConfig): DiskStorage =
      DiskStorage(
        ref,
        nxv.defaultStorage,
        1L,
        deprecated = false,
        default = true,
        config.disk.digestAlgorithm,
        config.disk.volume,
        config.disk.readPermission,
        config.disk.writePermission
      )
  }

  /**
    * An Amazon S3 compatible storage
    *
    * @param ref        a reference to the project that the store belongs to
    * @param id         the user facing store id
    * @param rev        the store revision
    * @param deprecated the deprecation state of the store
    * @param default    ''true'' if this store is the project's default backend, ''false'' otherwise
    * @param algorithm  the digest algorithm, e.g. "SHA-256"
    * @param bucket     the bucket
    * @param settings   an instance of [[S3Settings]] with proper credentials to access the bucket
    */
  final case class S3Storage(ref: ProjectRef,
                             id: AbsoluteIri,
                             rev: Long,
                             deprecated: Boolean,
                             default: Boolean,
                             algorithm: String,
                             bucket: String,
                             settings: S3Settings)
      extends Storage

  /**
    * S3 connection settings with reasonable defaults.
    *
    * @param credentials optional credentials
    * @param endpoint    optional endpoint, either a domain or a full URL
    * @param region      optional region
    */
  final case class S3Settings(credentials: Option[S3Credentials], endpoint: Option[String], region: Option[String]) {

    /**
      * @return these settings converted to an instance of [[akka.stream.alpakka.s3.S3Settings]]
      */
    def toAlpakka: s3.S3Settings = {
      val credsProvider = credentials match {
        case Some(S3Credentials(accessKey, secretKey)) =>
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
        case None => new AWSStaticCredentialsProvider(new AnonymousAWSCredentials)
      }

      val regionProvider = region match {
        case Some(reg) =>
          new AwsRegionProvider {
            val getRegion: String = reg
          }
        case None => new DefaultAwsRegionProviderChain()
      }

      val proxy = System
        .getenv()
        .asScala
        .collectFirst {
          case (k, v) if k.toLowerCase == "https_proxy" && v.nonEmpty => v
        }
        .flatMap(address => Try(Uri(address)).toOption)
        .map(uri => s3.Proxy(uri.authority.host.address, uri.effectivePort, uri.scheme))

      s3.S3Settings(MemoryBufferType,
                    proxy,
                    credsProvider,
                    regionProvider,
                    pathStyleAccess = true,
                    endpoint,
                    ApiVersion.ListBucketVersion2)
    }
  }

  /**
    * S3 credentials.
    *
    * @param accessKey the AWS access key ID
    * @param secretKey the AWS secret key
    */
  final case class S3Credentials(accessKey: String, secretKey: String)

  /**
    * Attempts to transform the resource into a [[Storage]].
    *
    * @param res a materialized resource
    * @return Right(storage) if the resource is compatible with a Storage, Left(rejection) otherwise
    */
  final def apply(res: ResourceV)(implicit config: StorageConfig): Either[Rejection, Storage] = {
    if (Set(nxv.Storage.value, nxv.DiskStorage.value).subsetOf(res.types)) diskStorage(res)
    else if (Set(nxv.Storage.value, nxv.Alpha.value, nxv.S3Storage.value).subsetOf(res.types)) s3Storage(res)
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the view types"))
  }

  private def diskStorage(res: ResourceV)(implicit config: StorageConfig): Either[Rejection, DiskStorage] = {
    val c = res.value.graph.cursor()
    for {
      default <- c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref)
      volume  <- c.downField(nxv.volume).focus.as[String].map(Paths.get(_)).toRejectionOnLeft(res.id.ref)
      readPerms   <- c.downField(nxv.readPermission).focus.as[Permission].orElse(config.disk.readPermission).toRejectionOnLeft(res.id.ref)
      writePerms  <- c.downField(nxv.writePermission).focus.as[Permission].orElse(config.disk.writePermission).toRejectionOnLeft(res.id.ref)
    } yield
      DiskStorage(res.id.parent, res.id.value, res.rev, res.deprecated, default, config.disk.digestAlgorithm, volume)
  }

  private def s3Storage(res: ResourceV)(implicit config: StorageConfig): Either[Rejection, S3Storage] = {
    val c = res.value.graph.cursor()
    for {
      default <- c.downField(nxv.default).focus.as[Boolean].toRejectionOnLeft(res.id.ref)
      bucket  <- c.downField(nxv.bucket).focus.as[String].toRejectionOnLeft(res.id.ref)
      endpoint = c.downField(nxv.endpoint).focus.flatMap(_.as[String].toOption)
      region   = c.downField(nxv.region).focus.flatMap(_.as[String].toOption)
      credentials = for {
        ak <- c.downField(nxv.accessKey).focus.flatMap(_.as[String].toOption)
        sk <- c.downField(nxv.secretKey).focus.flatMap(_.as[String].toOption)
      } yield S3Credentials(ak, sk)
    } yield
      S3Storage(res.id.parent,
                res.id.value,
                res.rev,
                res.deprecated,
                default,
                config.amazon.digestAlgorithm,
                bucket,
                S3Settings(credentials, endpoint, region))
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

  trait VerifyStorage[F[_]] {

    /**
      * Verifies a storage
      */
    def apply: F[Either[String, Unit]]
  }

  object StorageOperations {

    /**
      * Verifies that the provided storage can be used
      *
      * @tparam F the effect type
      */
    trait Verify[F[_]] {
      def apply(storage: Storage): VerifyStorage[F]
    }

    object Verify {
      implicit final def apply[F[_]: Effect](implicit as: ActorSystem): Verify[F] = {
        case s: DiskStorage => new DiskStorageOperations.VerifyDiskStorage[F](s)
        case s: S3Storage   => new S3StorageOperations.Verify[F](s)
      }
    }

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
        case _: DiskStorage => DiskStorageOperations.FetchDiskFile
        case s: S3Storage   => new S3StorageOperations.Fetch(s)
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
        case s: DiskStorage => new DiskStorageOperations.SaveDiskFile(s)
        case s: S3Storage   => new S3StorageOperations.Save(s)
      }
    }
  }
}
