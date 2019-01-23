package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import cats.implicits._
import cats.{Monad, MonadError}
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.FileConfig
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.LocationResolver.Location
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore._
import journal.Logger
import monix.eval.Task

import scala.concurrent.Future
import scala.util.control.NonFatal

class FileStore[F[_]: Monad, In, Out](implicit loc: LocationResolver[F], stream: Stream[F, In, Out]) {

  /**
    * Stores the provided stream source delegating to ''locator'' for choosing the location
    * and to ''fileStream'' for storing the source on the selected location.
    *
    * @param id       the id of the resource
    * @param fileDesc the file descriptor to be stored
    * @param source   the source
    * @return [[FileAttributes]] wrapped in the abstract ''F[_]''  type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save(id: ResId, fileDesc: FileDescription, source: In): F[FileAttributes] =
    loc(id, fileDesc.uuid).flatMap(location => stream.toSink(location, source, fileDesc)).map(fileDesc.process)

  /**
    * Fetches the file associated to the provided ''fileMeta'' delegating to ''locator'' for choosing the location
    * where to retrieve it and to ''fileStream'' for retrieving it.
    *
    * @param fileMeta the file metadata
    */
  def fetch(fileMeta: FileAttributes): Out =
    stream.toSource(loc.base.resolve(fileMeta.filePath))
}

object FileStore {

  type AkkaIn  = Source[ByteString, Any]
  type AkkaOut = Source[ByteString, Future[IOResult]]

  trait Stream[F[_], In, Out] {

    /**
      * Attempts to create a Out from a URI.
      * This should be used to transmit the content referred by the URI through the Out type in streaming fashion.
      *
      * @param path the [[Path]] from where to retrieve the content
      * @return the typeclass Out
      */
    def toSource(path: Path): Out

    /**
      * Attempts to store and create metadata information which will be used by [[ch.epfl.bluebrain.nexus.kg.resources.State]]
      * from an incoming source of type In (which is typically a stream).
      *
      * @param loc    the location of the file
      * @param source the source
      * @param meta   the source metadata
      */
    def toSink(loc: Location, source: In, meta: FileDescription): F[StoredSummary]
  }

  object Stream {

    /**
      * Construct a Stream based on Akka Streams
      *
      * @param config the files configuration
      */
    def akka(config: FileConfig)(implicit as: ActorSystem): Stream[Future, AkkaIn, AkkaOut] =
      new Stream[Future, AkkaIn, AkkaOut] {
        import as.dispatcher
        implicit val mt: Materializer = ActorMaterializer()

        def toSource(path: Path): AkkaOut =
          FileIO.fromPath(path)

        def toSink(loc: Location, source: AkkaIn, meta: FileDescription): Future[StoredSummary] =
          source
            .alsoToMat(digestSink)(Keep.right)
            .toMat(FileIO.toPath(loc.path)) {
              case (digFuture, ioFuture) =>
                digFuture.zipWith(ioFuture) {
                  case (dig, io) if io.wasSuccessful && loc.path.toFile.exists() =>
                    val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
                    Future(StoredSummary(loc.relative, io.count, digest))
                  case _ =>
                    Future.failed(KgError.InternalError(
                      s"I/O error writing file with contentType '${meta.mediaType}' and filename '${meta.filename}'"))
                }
            }
            .run()
            .flatten

        private def digestSink: Sink[ByteString, Future[MessageDigest]] =
          Sink.fold(MessageDigest.getInstance(config.digestAlgorithm))((digest, currentBytes) => {
            digest.update(currentBytes.asByteBuffer)
            digest
          })
      }

    /**
      * Construct a Stream based on Akka Streams wrapped on a [[Task]]
      *
      * @param config the files configuration
      */
    def task(config: FileConfig)(implicit as: ActorSystem): Stream[Task, AkkaIn, AkkaOut] =
      new Stream[Task, AkkaIn, AkkaOut] {
        private val underlying = akka(config)

        def toSource(path: Path): AkkaOut =
          underlying.toSource(path)

        def toSink(loc: Location, source: AkkaIn, meta: FileDescription): Task[StoredSummary] =
          Task.deferFuture(underlying.toSink(loc, source, meta))
      }

  }

  /**
    * Manages the location of files from a provided ''base''.
    *
    * @param base path of the root directory where to store files
    * @tparam F the monadic effect type
    */
  abstract class LocationResolver[F[_]](private[file] val base: Path) {

    /**
      * Attempts to create a location for the file.
      *
      * @param id   the id of the resource
      * @param uuid the file unique identifier
      * @return ''Location'' or the appropriate Fault in the ''F'' context
      */
    def apply(id: ResId, uuid: String): F[Location]
  }

  object LocationResolver {

    /**
      * Wraps both the absolute and relative information about the file location
      *
      * @param path     absolute Path where to find the file
      * @param relative relative route to the file location
      */
    final case class Location(path: Path, relative: Path)

    /**
      * Constructs a ''LocationResolver'' from base path implementing
      * the missing apply method.
      *
      * @param base path of the root directory from where to store files
      * @return the LocationResolver
      */
    def apply[F[_]](base: Path)(implicit F: MonadError[F, Throwable]): LocationResolver[F] =
      new LocationResolver[F](base) {
        val logger: Logger = Logger[this.type]
        override def apply(id: ResId, uuid: String): F[Location] = {
          F.catchNonFatal {
              val relative = Paths.get(s"${id.parent.id}/${uuid.takeWhile(_ != '-').mkString("/")}/$uuid")
              val filePath = base.resolve(relative)
              Files.createDirectories(filePath.getParent)
              Location(filePath, relative)
            }
            .recoverWith {
              case NonFatal(err) =>
                logger.error(s"Unable to derive Location for path '$base'", err)
                F.raiseError(KgError.InternalError(s"Unable to derive Location for path '$base'"))
            }
        }
      }

    def apply[F[_]]()(implicit F: MonadError[F, Throwable], config: FileConfig): LocationResolver[F] =
      apply(config.volume)
  }

}
