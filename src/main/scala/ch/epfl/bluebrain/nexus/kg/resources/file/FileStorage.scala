package ch.epfl.bluebrain.nexus.kg.resources.file
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep}
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription, StoredSummary}
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResId}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class FileStorage(ref: ProjectRef,
                       id: AbsoluteIri,
                       uuid: UUID,
                       rev: Long,
                       instant: Instant,
                       deprecated: Boolean,
                       default: Boolean,
                       digestAlgorithm: String,
                       volume: Path)
    extends Storage

object FileStorage {

  private val logger: Logger = Logger[this.type]

  /**
    * Wraps both the absolute and relative information about the file location
    *
    * @param path     absolute Path where to find the file
    * @param relative relative route to the file location
    */
  final case class Location(path: Path, relative: Path)

  implicit def storageOps[F[_]](storage: FileStorage)(
      implicit F: Effect[F],
      as: ActorSystem): StorageOperations[F, FileStorage, AkkaIn, AkkaOut] =
    new StorageOperations[F, FileStorage, AkkaIn, AkkaOut] {

      override def save(id: ResId, fileDesc: FileDescription, source: AkkaIn): F[FileAttributes] = {
        implicit val ec: ExecutionContext = as.dispatcher
        implicit val mt: Materializer     = ActorMaterializer()

        getLocation(id, fileDesc.uuid).flatMap { loc =>
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

      private def getLocation(id: ResId, uuid: String): F[Location] = {
        F.catchNonFatal {
            val relative = Paths.get(s"${id.parent.id}/${uuid.takeWhile(_ != '-').mkString("/")}/$uuid")
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
