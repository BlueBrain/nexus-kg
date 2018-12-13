package ch.epfl.bluebrain.nexus.kg.resources.binary

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import cats.data.EitherT
import cats.{Applicative, Monad}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.BinaryConfig
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
import ch.epfl.bluebrain.nexus.kg.resources.binary.Binary._
import ch.epfl.bluebrain.nexus.kg.resources.binary.BinaryStore.LocationResolver.Location
import ch.epfl.bluebrain.nexus.kg.resources.binary.BinaryStore._
import ch.epfl.bluebrain.nexus.kg.resources.{Rejection, ResId}
import monix.eval.Task

import scala.concurrent.Future
import scala.util.Try

class BinaryStore[F[_]: Monad, In, Out](implicit loc: LocationResolver[F], stream: Stream[F, In, Out]) {

  /**
    * Stores the provided stream source delegating to ''locator'' for choosing the location
    * and to ''fileStream'' for storing the source on the selected location.
    *
    * @param id     the id of the resource
    * @param binary the binary to be stored
    * @param source the source
    * @return [[BinaryAttributes]] wrapped in the abstract ''F[_]''  type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save(id: ResId, binary: BinaryDescription, source: In): EitherT[F, Rejection, BinaryAttributes] =
    loc(id, binary.uuid).flatMap(location => stream.toSink(location, source, binary)).map(binary.process)

  /**
    * Fetches the binary associated to the provided ''binary'' delegating to ''locator'' for choosing the location
    * where to retrieve it and to ''fileStream'' for retrieving it.
    *
    * @param binary the binary metadata
    */
  def fetch(binary: BinaryAttributes): Either[Rejection, Out] =
    stream.toSource(loc.base.resolve(binary.filePath))
}

object BinaryStore {

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
    def toSource(path: Path): Either[Rejection, Out]

    /**
      * Attempts to store and create metadata information which will be used by [[ch.epfl.bluebrain.nexus.kg.resources.State]]
      * from an incoming source of type In (which is typically a stream).
      *
      * @param loc    the location of the binary
      * @param source the source
      * @param meta   the source metadata
      */
    def toSink(loc: Location, source: In, meta: BinaryDescription): EitherT[F, Rejection, StoredSummary]
  }

  object Stream {

    /**
      * Construct a Stream based on Akka Streams
      *
      * @param config the binaries configuration
      */
    def akka(config: BinaryConfig)(implicit as: ActorSystem): Stream[Future, AkkaIn, AkkaOut] =
      new Stream[Future, AkkaIn, AkkaOut] {
        import as.dispatcher
        implicit val mt: Materializer = ActorMaterializer()

        def toSource(path: Path): Either[Rejection, AkkaOut] =
          Try(FileIO.fromPath(path)).toEither.left.map[Rejection](th => Unexpected(th.getMessage))

        def toSink(loc: Location, source: AkkaIn, meta: BinaryDescription): EitherT[Future, Rejection, StoredSummary] =
          EitherT(source
            .alsoToMat(digestSink)(Keep.right)
            .toMat(FileIO.toPath(loc.path)) {
              case (digFuture, ioFuture) =>
                digFuture.zipWith(ioFuture) {
                  case (dig, io) if io.wasSuccessful && loc.path.toFile.exists() =>
                    val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
                    Future(Right(StoredSummary(loc.relative, io.count, digest)))
                  case _ =>
                    Future(Left(Unexpected(
                      s"I/O error writing binary with contentType '${meta.mediaType}' and filename '${meta.filename}'"): Rejection))
                }
            }
            .run()
            .flatten)

        private def digestSink: Sink[ByteString, Future[MessageDigest]] =
          Sink.fold(MessageDigest.getInstance(config.digestAlgorithm))((digest, currentBytes) => {
            digest.update(currentBytes.asByteBuffer)
            digest
          })
      }

    /**
      * Construct a Stream based on Akka Streams wrapped on a [[Task]]
      *
      * @param config the binaries configuration
      */
    def task(config: BinaryConfig)(implicit as: ActorSystem): Stream[Task, AkkaIn, AkkaOut] =
      new Stream[Task, AkkaIn, AkkaOut] {
        private val underlying = akka(config)

        def toSource(path: Path): Either[Rejection, AkkaOut] =
          underlying.toSource(path)

        def toSink(loc: Location, source: AkkaIn, meta: BinaryDescription): EitherT[Task, Rejection, StoredSummary] =
          EitherT(Task.deferFuture(underlying.toSink(loc, source, meta).value))
      }

  }

  /**
    * Manages the location of binaries from a provided ''base''.
    *
    * @param base path of the root directory where to store binaries
    *
    * @tparam F the monadic effect type
    */
  abstract class LocationResolver[F[_]](private[binary] val base: Path) {

    /**
      * Attempts to create a location for the binary.
      *
      * @param id   the id of the resource
      * @param uuid the binary unique identifier
      * @return ''Location'' or the appropriate Fault in the ''F'' context
      */
    def apply(id: ResId, uuid: String): EitherT[F, Rejection, Location]
  }

  object LocationResolver {

    /**
      * Wraps both the absolute and relative information about the binary location
      *
      * @param path     absolute Path where to find the binary
      * @param relative relative route to the binary location
      */
    final case class Location(path: Path, relative: Path)

    /**
      * Constructs a ''LocationResolver'' from base path implementing
      * the missing apply method.
      *
      * @param base path of the root directory from where to store binaries
      * @return the LocationResolver
      */
    def apply[F[_]: Applicative](base: Path): LocationResolver[F] =
      new LocationResolver[F](base) {
        override def apply(id: ResId, uuid: String): EitherT[F, Rejection, Location] = {
          EitherT.fromEither[F](
            Try {
              val relative       = Paths.get(s"${id.parent.id}/${uuid.takeWhile(_ != '-').mkString("/")}/$uuid")
              val attachmentPath = base.resolve(relative)
              Files.createDirectories(attachmentPath.getParent)
              Location(attachmentPath, relative)
            }.toEither.left.map[Rejection](th => Unexpected(th.getMessage))
          )
        }
      }

    def apply[F[_]: Applicative]()(implicit config: BinaryConfig): LocationResolver[F] =
      apply(config.volume)

  }

}
