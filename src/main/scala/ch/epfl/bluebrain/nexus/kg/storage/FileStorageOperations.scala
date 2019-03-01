package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep}
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{FetchFile, FileStorage, SaveFile}
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object FileStorageOperations {

  /**
    * [[FetchFile]] implementation for [[FileStorage]]
    *
    * @param storage the file storage
    */
  final class Fetch(storage: FileStorage) extends FetchFile[AkkaOut] {

    override def apply(fileMeta: FileAttributes): AkkaOut =
      FileIO.fromPath(storage.volume.resolve(fileMeta.filePath))
  }

  /**
    * [[SaveFile]] implementation for [[FileStorage]]
    *
    * @param storage the file storage
    */
  final class Save[F[_]](storage: FileStorage)(implicit F: Effect[F], as: ActorSystem) extends SaveFile[F, AkkaIn] {

    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val mt: Materializer     = ActorMaterializer()
    private val logger: Logger                = Logger[this.type]

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaIn): F[FileAttributes] = {
      getLocation(fileDesc.uuid).flatMap {
        case (fullPath, relativePath) =>
          val future = source
            .alsoToMat(digestSink(storage.algorithm))(Keep.right)
            .toMat(FileIO.toPath(fullPath)) {
              case (digFuture, ioFuture) =>
                digFuture.zipWith(ioFuture) {
                  case (dig, io) if io.wasSuccessful && fullPath.toFile.exists() =>
                    val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
                    Future(fileDesc.process(StoredSummary(relativePath, io.count, digest)))
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

    private def getLocation(uuid: String): F[(Path, Path)] = {
      F.catchNonFatal {
          val relative = Paths.get(s"${uuid.takeWhile(_ != '-').mkString("/")}/$uuid")
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
