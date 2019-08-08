package ch.epfl.bluebrain.nexus.kg.storage

import akka.http.scaladsl.model.Uri
import cats.Applicative
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.StorageConfig
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import ch.epfl.bluebrain.nexus.storage.client.types.FileAttributes.{Digest => StorageDigest}
import ch.epfl.bluebrain.nexus.storage.client.types.{FileAttributes => StorageFileAttributes}
import com.github.ghik.silencer.silent

object RemoteDiskStorageOperations {

  /**
    * [[VerifyStorage]] implementation for [[RemoteDiskStorage]]
    *
    * @param storage the [[RemoteDiskStorage]]
    * @param client  the remote storage client
    */
  final class Verify[F[_]: Applicative](storage: RemoteDiskStorage, client: StorageClient[F])(
      implicit config: StorageConfig
  ) extends VerifyStorage[F] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply: F[Either[String, Unit]] = client.exists(storage.folder).map {
      case true  => Right(())
      case false => Left(s"Folder '${storage.folder}' does not exists on the endpoint '${storage.endpoint}'")
    }
  }

  /**
    * [[FetchFile]] implementation for [[RemoteDiskStorage]]
    *
    * @param storage the [[RemoteDiskStorage]]
    * @param client  the remote storage client
    */
  final class Fetch[F[_]](storage: RemoteDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends FetchFile[F, AkkaSource] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(fileMeta: FileAttributes): F[AkkaSource] =
      client.getFile(storage.folder, fileMeta.path)

  }

  /**
    * [[SaveFile]] implementation for [[RemoteDiskStorage]]
    *
    * @param storage the [[RemoteDiskStorage]]
    * @param client  the remote storage client
    */
  final class Save[F[_]: Effect](storage: RemoteDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends SaveFile[F, AkkaSource] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaSource): F[FileAttributes] = {
      val relativePath = Uri.Path(mangle(storage.ref, fileDesc.uuid, fileDesc.filename))
      client.createFile(storage.folder, relativePath, source).map {
        case StorageFileAttributes(location, bytes, StorageDigest(algorithm, hash)) =>
          val dig = Digest(algorithm, hash)
          FileAttributes(fileDesc.uuid, location, relativePath, fileDesc.filename, fileDesc.mediaType, bytes, dig)
      }
    }
  }

  /**
    * [[LinkFile]] implementation for [[RemoteDiskStorage]]
    *
    * @param storage the [[RemoteDiskStorage]]
    * @param client  the remote storage client
    */
  final class Link[F[_]: Effect](storage: RemoteDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends LinkFile[F] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(id: ResId, fileDesc: FileDescription, path: Uri.Path): F[FileAttributes] = {
      val destRelativePath = Uri.Path(mangle(storage.ref, fileDesc.uuid, fileDesc.filename))
      client.moveFile(storage.folder, path, destRelativePath).map {
        case StorageFileAttributes(location, bytes, StorageDigest(algorithm, hash)) =>
          val dig = Digest(algorithm, hash)
          FileAttributes(fileDesc.uuid, location, destRelativePath, fileDesc.filename, fileDesc.mediaType, bytes, dig)
      }
    }
  }

  /**
    * [[FetchFileDigest]] implementation for [[RemoteDiskStorage]]
    *
    * @param storage the [[RemoteDiskStorage]]
    * @param client  the remote storage client
    */
  @silent
  final class FetchDigest[F[_]: Effect](storage: RemoteDiskStorage, client: StorageClient[F])(
      implicit config: StorageConfig
  ) extends FetchFileDigest[F] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(relativePath: Uri.Path): F[Digest] =
      client.getDigest(storage.folder, relativePath).map {
        case StorageDigest(algorithm, value) => Digest(algorithm, value)
      }
  }

}
