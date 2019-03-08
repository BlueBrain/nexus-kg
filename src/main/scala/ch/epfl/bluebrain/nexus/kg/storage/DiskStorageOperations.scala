package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Source}
import akka.stream.{ActorMaterializer, Materializer}
import cats.Monad
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, FetchFile, SaveFile, VerifyStorage}
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object DiskStorageOperations {

  private val logger = Logger[this.type]

  /**
    * [[VerifyStorage]] implementation for [[DiskStorage]]
    *
    * @param storage the [[DiskStorage]]
    */
  final class VerifyDiskStorage[F[_]](storage: DiskStorage)(implicit F: Monad[F]) extends VerifyStorage[F] {
    override def apply: F[Either[String, Unit]] =
      if (!Files.exists(storage.volume)) F.pure(Left(s"Volume '${storage.volume}' does not exist."))
      else if (!Files.isDirectory(storage.volume)) F.pure(Left(s"Volume '${storage.volume}' is not a directory."))
      else if (!Files.isWritable(storage.volume))
        F.pure(Left(s"Volume '${storage.volume}' does not have write access."))
      else F.pure(Right(()))
  }

  /**
    * [[FetchFile]] implementation for [[DiskStorage]]
    *
    */
  object FetchDiskFile extends FetchFile[AkkaSource] {

    override def apply(fileMeta: FileAttributes): AkkaSource = uriToPath(fileMeta.location) match {
      case Some(path) => FileIO.fromPath(path)
      case None =>
        logger.error(s"Invalid file location: '${fileMeta.location}'")
        Source.empty
    }
  }

  /**
    * [[SaveFile]] implementation for [[DiskStorage]]
    *
    * @param storage the [[DiskStorage]]
    */
  final class SaveDiskFile[F[_]](storage: DiskStorage)(implicit F: Effect[F], as: ActorSystem)
      extends SaveFile[F, AkkaSource] {

    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val mt: Materializer     = ActorMaterializer()

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaSource): F[FileAttributes] = {
      getLocation(fileDesc.uuid).flatMap {
        case (fullPath, relativePath) =>
          val future = source
            .alsoToMat(digestSink(storage.algorithm))(Keep.right)
            .toMat(FileIO.toPath(fullPath)) {
              case (digFuture, ioFuture) =>
                digFuture.zipWith(ioFuture) {
                  case (dig, io) if io.wasSuccessful && fullPath.toFile.exists() =>
                    val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
                    Future(fileDesc.process(StoredSummary(relativePath.toString, io.count, digest)))
                  case _ =>
                    Future.failed(KgError.InternalError(
                      s"I/O error writing file with contentType '${fileDesc.mediaType}' and filename '${fileDesc.filename}'"))
                }
            }
            .run()
            .flatten

          F.liftIO(IO.fromFuture(IO(future)))
      }
    }

    private def getLocation(uuid: UUID): F[(Path, Path)] = {
      F.catchNonFatal {
          val relative = Paths.get(mangle(storage.ref, uuid))
          val filePath = storage.volume.resolve(relative)
          Files.createDirectories(filePath.getParent)
          (filePath, relative)
        }
        .recoverWith {
          case NonFatal(err) =>
            logger.error(s"Unable to derive Location for path '${storage.volume}'", err)
            F.raiseError(KgError.InternalError(s"Unable to derive Location for path '${storage.volume}'"))
        }
    }
  }

}
