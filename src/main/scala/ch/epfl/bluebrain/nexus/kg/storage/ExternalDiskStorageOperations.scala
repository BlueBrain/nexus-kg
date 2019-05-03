package ch.epfl.bluebrain.nexus.kg.storage

import akka.http.scaladsl.model.Uri
import cats.Applicative
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.StorageConfig
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{ExternalDiskStorage, FetchFile, LinkFile, SaveFile, VerifyStorage}
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import ch.epfl.bluebrain.nexus.storage.client.types.FileAttributes.{Digest => StorageDigest}
import ch.epfl.bluebrain.nexus.storage.client.types.{FileAttributes => StorageFileAttributes}

object ExternalDiskStorageOperations {

  /**
    * [[VerifyStorage]] implementation for [[ExternalDiskStorage]]
    *
    * @param storage the [[ExternalDiskStorage]]
    * @param client  the external storage client
    */
  final class Verify[F[_]: Applicative](storage: ExternalDiskStorage, client: StorageClient[F])(
      implicit config: StorageConfig)
      extends VerifyStorage[F] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply: F[Either[String, Unit]] = client.exists(storage.folder).map {
      case true  => Right(())
      case false => Left(s"Folder '${storage.folder}' does not exists on the endpoint '${storage.endpoint}'")
    }
  }

  /**
    * [[FetchFile]] implementation for [[ExternalDiskStorage]]
    *
    * @param storage the [[ExternalDiskStorage]]
    * @param client  the external storage client
    */
  final class Fetch[F[_]](storage: ExternalDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends FetchFile[F, AkkaSource] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(fileMeta: FileAttributes): F[AkkaSource] =
      client.getFile(storage.folder, fileMeta.path)

  }

  /**
    * [[SaveFile]] implementation for [[ExternalDiskStorage]]
    *
    * @param storage the [[ExternalDiskStorage]]
    * @param client  the external storage client
    */
  final class Save[F[_]: Effect](storage: ExternalDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends SaveFile[F, AkkaSource] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaSource): F[FileAttributes] = {
      val relativePath = Uri.Path(mangle(storage.ref, fileDesc.uuid))
      client.createFile(storage.folder, relativePath, fileDesc.filename, fileDesc.mediaType, source).map {
        case StorageFileAttributes(location, filename, mediaType, bytes, StorageDigest(algorithm, hash)) =>
          FileAttributes(fileDesc.uuid, location, relativePath, filename, mediaType, bytes, Digest(algorithm, hash))
      }
    }
  }

  /**
    * [[LinkFile]] implementation for [[ExternalDiskStorage]]
    *
    * @param storage the [[ExternalDiskStorage]]
    * @param client  the external storage client
    */
  final class Link[F[_]: Effect](storage: ExternalDiskStorage, client: StorageClient[F])(implicit config: StorageConfig)
      extends LinkFile[F] {
    implicit val cred = storage.decryptAuthToken(config.derivedKey)

    override def apply(id: ResId, fileDesc: FileDescription, path: Uri.Path): F[FileAttributes] = {
      val destRelativePath = Uri.Path(mangle(storage.ref, fileDesc.uuid))
      client.moveFile(storage.folder, path, destRelativePath, fileDesc.filename, fileDesc.mediaType).map {
        case StorageFileAttributes(location, filename, mediaType, bytes, StorageDigest(algorithm, hash)) =>
          FileAttributes(fileDesc.uuid, location, destRelativePath, filename, mediaType, bytes, Digest(algorithm, hash))
      }
    }
  }

}
