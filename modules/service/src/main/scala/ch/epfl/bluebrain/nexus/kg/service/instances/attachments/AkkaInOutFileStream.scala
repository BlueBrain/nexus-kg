package ch.epfl.bluebrain.nexus.kg.service.instances.attachments

import java.net.URI
import java.nio.file.{Path, Paths}
import java.security.MessageDigest

import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.core.Fault.Unexpected
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment.{Digest, Info, Meta, Size}
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.{AttachmentLocation, InOutFileStream}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class AkkaInOutFileStream(algorithm: String)(implicit
  mt: Materializer,
  ec: ExecutionContext,
  F: MonadError[Future, Throwable],
  al: AttachmentLocation[Future])
  extends InOutFileStream[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]] {

  /**
    * Generates a Source from a path.
    *
    * @param uri the URI from where to retrieve the content
    * @return the typeclass Out
    */
  override def toSource(uri: URI): Future[Source[ByteString, Future[IOResult]]] = {
    Try {
      FileIO.fromPath(Paths.get(uri))
    }.fold(
      // $COVERAGE-OFF$
      failed => F.raiseError(Unexpected(s"Attachment could not be found '${failed.getMessage}'")),
      // $COVERAGE-ON$
      source => F.pure(source)
    )
  }

  /**
    * This process can be represented as follows:
    *
    * Source[ByteString] consumed by two sinks (ioSink and digestSink) and zipped into one future.
    * When the Future is completed, the metadata data type is created with the information generated from the sinks.
    *
    * @param id          the unique identifier of the instance
    * @param rev         the instance revision number
    * @param filename    the filename of the source
    * @param contentType the content type of the source asserted by the client
    * @param source      the source of data
    * @return the metadata information of the source
    *         or the appropriate Fault in the ''F'' context
    */
  override def toSink(id: InstanceId, rev: Long, filename: String, contentType: String, source: Source[ByteString, Any]): Future[Meta] = {
    al.apply(id, rev).flatMap { loc =>
      source
        .alsoToMat(digestSink)(Keep.right)
        .toMat(ioSink(loc.path)) { case (digFuture, ioFuture) =>
          digFuture.zipWith(ioFuture) { case (dig, io) =>
            if (io.wasSuccessful && loc.path.toFile.exists()) {
              val digest = Digest(dig.getAlgorithm, dig.digest().map("%02x".format(_)).mkString)
              Right(Meta(loc.relative, Info(filename, contentType, Size(value = io.count), digest)))
            } else {
              // $COVERAGE-OFF$
              Left(Unexpected(s"I/O error while writting attachment for id '$id' and contentType '$contentType' and filename '$filename'"))
              // $COVERAGE-ON$
            }
          }
        }
        .run().flatMap {
        case Left(rejection) => F.raiseError(rejection)
        case Right(meta)     => F.pure(meta)
      }
    }
  }

  /**
    * Sinks which generates a File from a Source of ByteString.
    *
    * @param path the path where to Sink
    * @return ''Future[IOResult]'' that contains information about the Sink process
    */
  private def ioSink(path: Path): Sink[ByteString, Future[IOResult]] = {
    FileIO.toPath(path)
  }

  /**
    * Sinks which generates a Digest from a Source of ByteString.
    *
    * @return ''Future[MessageDigest]'' that contains the digest information
    */
  private def digestSink: Sink[ByteString, Future[MessageDigest]] = {
    val initDigest = MessageDigest.getInstance(algorithm)
    Sink.fold[MessageDigest, ByteString](initDigest)((digest, currentBytes) => {
      digest.update(currentBytes.asByteBuffer)
      digest
    })
  }
}

object AkkaInOutFileStream {

  /**
    * Constructs AkkaInOutFileStream.
    *
    * @param algorithm the algorithm used to compute the digest
    * @param mt        the underlying materializer
    * @param ec        the underlying execution context
    * @param F         an implicitly available MonadError typeclass for ''Future''
    * @param al        the available attachment location
    * @return AkkaInOutFileStream
    */
  def apply(algorithm: String)(implicit
    mt: Materializer,
    ec: ExecutionContext,
    F: MonadError[Future, Throwable],
    al: AttachmentLocation[Future]): AkkaInOutFileStream = new AkkaInOutFileStream(algorithm)

  /**
    * Constructs AkkaInOutFileStream.
    *
    * @param settings the application settings
    * @param mt       the underlying materializer
    * @param ec       the underlying execution context
    * @param F        an implicitly available MonadError typeclass for ''Future''
    * @param al       the available attachment location
    * @return AkkaInOutFileStream
    */
  def apply(settings: Settings)(implicit
    mt: Materializer,
    ec: ExecutionContext,
    F: MonadError[Future, Throwable],
    al: AttachmentLocation[Future]): AkkaInOutFileStream = apply(settings.Attachment.HashAlgorithm)(mt, ec, F, al)

}