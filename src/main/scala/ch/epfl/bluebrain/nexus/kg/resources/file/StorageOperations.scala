package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep}
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription, StoredSummary}
import ch.epfl.bluebrain.nexus.kg.resources.file.Storage.{FileStorage, S3Storage}
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * Contract for storage operations.
  *
  * @tparam F         the effect type
  * @tparam In        the input type
  * @tparam Out       the output type
  */
trait StorageOperations[F[_], In, Out] {

  /**
    * Stores the provided stream source.
    *
    * @param id       the id of the resource
    * @param fileDesc the file descriptor to be stored
    * @param source   the source
    * @return [[FileAttributes]] wrapped in the abstract ''F[_]'' type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save(id: ResId, fileDesc: FileDescription, source: In): F[FileAttributes]

  /**
    * Fetches the file associated to the provided ''fileMeta''.
    *
    * @param fileMeta the file metadata
    */
  def fetch(fileMeta: FileAttributes): Out

}

object StorageOperations {
  private val logger: Logger = Logger[this.type]

  /**
    * Wraps both the absolute and relative information about the file location
    *
    * @param path     absolute Path where to find the file
    * @param relative relative route to the file location
    */
  private final case class Location(path: Path, relative: Path)

  implicit def fileStorage[F[_]: Effect](implicit storage: Storage,
                                         as: ActorSystem): StorageOperations[F, AkkaIn, AkkaOut] =
    storage match {
      case value: FileStorage => fileStorage(value)
      case value: S3Storage   => s3Storage(value)
    }

  private def s3Storage[F[_]](storage: S3Storage)(implicit
                                                  F: Effect[F],
                                                  as: ActorSystem): StorageOperations[F, AkkaIn, AkkaOut] = ???

  private def fileStorage[F[_]](storage: FileStorage)(implicit
                                                      F: Effect[F],
                                                      as: ActorSystem): StorageOperations[F, AkkaIn, AkkaOut] =
    new StorageOperations[F, AkkaIn, AkkaOut] {

      override def save(id: ResId, fileDesc: FileDescription, source: AkkaIn): F[FileAttributes] = {
        implicit val ec: ExecutionContext = as.dispatcher
        implicit val mt: Materializer     = ActorMaterializer()

        getLocation(fileDesc.uuid).flatMap { loc =>
          val future = source
            .alsoToMat(digestSink(storage.digestAlgorithm))(Keep.right)
            .toMat(FileIO.toPath(loc.path)) {
              case (digFuture, ioFuture) =>
                digFuture.zipWith(ioFuture) {
                  case (dig, io) if io.wasSuccessful && loc.path.toFile.exists() =>
                    val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
                    Future(fileDesc.process(StoredSummary(loc.relative, io.count, digest)))
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

      override def fetch(fileMeta: FileAttributes): AkkaOut = {
        FileIO.fromPath(storage.volume.resolve(fileMeta.filePath))
      }

      private def getLocation(uuid: String): F[Location] = {
        F.catchNonFatal {
            val relative = Paths.get(s"${uuid.takeWhile(_ != '-').mkString("/")}/$uuid")
            val filePath = storage.volume.resolve(relative)
            Files.createDirectories(filePath.getParent)
            Location(filePath, relative)
          }
          .recoverWith {
            case NonFatal(err) =>
              logger.error(s"Unable to derive Location for path '${storage.volume}'", err)
              F.raiseError(KgError.InternalError(s"Unable to derive Location for path '${storage.volume}'"))
          }
      }

    }
}
