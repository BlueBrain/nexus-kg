package ch.epfl.bluebrain.nexus.kg.resources.attachment

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import cats.data.EitherT
import cats.{Applicative, Monad}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.AttachmentsConfig
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.LocationResolver.Location
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore._
import ch.epfl.bluebrain.nexus.kg.resources.{Rejection, ResId}
import monix.eval.Task

import scala.concurrent.Future
import scala.util.Try

class AttachmentStore[F[_]: Monad, In, Out](implicit loc: LocationResolver[F], stream: Stream[F, In, Out]) {

  /**
    * Stores the provided stream source delegating to ''locator'' for choosing the location
    * and to ''fileStream'' for storing the source on the selected location.
    *
    * @param id       the id of the resource
    * @param att              the attachment to be stored
    * @param source           the source
    * @return [[BinaryAttributes]] wrapped in the abstract ''F[_]''  type if successful,
    *         or a [[ch.epfl.bluebrain.nexus.kg.resources.Rejection]] wrapped within ''F[_]'' otherwise
    */
  def save(id: ResId, att: BinaryDescription, source: In): EitherT[F, Rejection, BinaryAttributes] =
    loc(id, att.uuid).flatMap(location => stream.toSink(location, source, att)).map(att.process)

  /**
    * Fetches the binary associated to the provided ''attachment'' delegating to ''locator'' for choosing the location
    * where to retrieve it and to ''fileStream'' for retrieving it.
    *
    * @param attachment the attachment metadata
    */
  def fetch(attachment: BinaryAttributes): Either[Rejection, Out] =
    stream.toSource(loc.base.resolve(attachment.filePath))
}

object AttachmentStore {

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
      * @param loc    the location of the attachment
      * @param source the source
      * @param meta   the source metadata
      */
    def toSink(loc: Location, source: In, meta: BinaryDescription): EitherT[F, Rejection, StoredSummary]
  }

  object Stream {

    /**
      * Construct a Stream based on Akka Streams
      *
      * @param config the attachment configuration
      */
    def akka(config: AttachmentsConfig)(implicit as: ActorSystem): Stream[Future, AkkaIn, AkkaOut] =
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
                      s"I/O error writing attachment with contentType '${meta.mediaType}' and filename '${meta.filename}'"): Rejection))
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
      * @param config the attachment configuration
      */
    def task(config: AttachmentsConfig)(implicit as: ActorSystem): Stream[Task, AkkaIn, AkkaOut] =
      new Stream[Task, AkkaIn, AkkaOut] {
        private val underlying = akka(config)

        def toSource(path: Path): Either[Rejection, AkkaOut] =
          underlying.toSource(path)

        def toSink(loc: Location, source: AkkaIn, meta: BinaryDescription): EitherT[Task, Rejection, StoredSummary] =
          EitherT(Task.deferFuture(underlying.toSink(loc, source, meta).value))
      }

  }

  /**
    * Manages the location of attachments from a provided ''base''.
    *
    * @param base path of the root directory where to store attachments
    *
    * @tparam F the monadic effect type
    */
  abstract class LocationResolver[F[_]](private[attachment] val base: Path) {

    /**
      * Attempts to create a location for an attachment.
      *
      * @param id   the id of the resource
      * @param uuid the attachment unique identifier
      * @return ''Location'' or the appropriate Fault in the ''F'' context
      */
    def apply(id: ResId, uuid: String): EitherT[F, Rejection, Location]
  }

  object LocationResolver {

    /**
      * Wraps both the absolute and relative information about the attachment's location
      *
      * @param path     absolute Path where to find the attachment
      * @param relative relative route to the attachment's location
      */
    final case class Location(path: Path, relative: Path)

    /**
      * Constructs a ''LocationResolver'' from base path implementing
      * the missing apply method.
      *
      * @param base path of the root directory from where to store attachments
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

    def apply[F[_]: Applicative]()(implicit config: AttachmentsConfig): LocationResolver[F] =
      apply(config.volume)

  }

}
