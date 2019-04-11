package ch.epfl.bluebrain.nexus.kg.storage

import java.net.URLDecoder
import java.util.NoSuchElementException

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Path._
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect._
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.StorageConfig
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}

object S3StorageOperations {

  private val logger = Logger[this.type]

  final class Verify[F[_]](storage: S3Storage)(implicit F: Effect[F], as: ActorSystem, config: StorageConfig)
      extends VerifyStorage[F] {

    private implicit val mt: Materializer = ActorMaterializer()

    override def apply: F[Either[String, Unit]] = {
      val future = IO(
        S3.listBucket(storage.bucket, None)
          .withAttributes(S3Attributes.settings(storage.settings.toAlpakka(config.derivedKey)))
          .runWith(Sink.head))
      IO.fromFuture(future)
        .attempt
        .map {
          case Right(_)                        => Right(())
          case Left(_: NoSuchElementException) => Right(()) // bucket is empty, that is fine
          case Left(e)                         => Left(s"Error accessing S3 bucket '${storage.bucket}': ${e.getMessage}")
        }
        .to[F]
    }
  }

  final class Fetch[F[_]](storage: S3Storage)(implicit F: Effect[F], as: ActorSystem, config: StorageConfig)
      extends FetchFile[F, AkkaSource] {

    private implicit val mt: Materializer = ActorMaterializer()

    override def apply(fileMeta: FileAttributes): F[AkkaSource] =
      getKey(storage.bucket, fileMeta.location.path) match {
        case Some(key) =>
          val future = IO(
            S3.download(storage.bucket, key)
              .withAttributes(S3Attributes.settings(storage.settings.toAlpakka(config.derivedKey)))
              .runWith(Sink.head))
          IO.fromFuture(future)
            .flatMap {
              case Some((source, _)) => IO.pure(source: AkkaSource)
              case None =>
                IO.raiseError(
                  KgError.InternalError(
                    s"Empty content fetching S3 object with key '$key' in bucket '${storage.bucket}'"))
            }
            .handleErrorWith {
              case e: KgError => IO.raiseError(e)
              case e: Throwable =>
                IO.raiseError(
                  KgError.DownstreamServiceError(
                    s"Error fetching S3 object with key '$key' in bucket '${storage.bucket}': ${e.getMessage}"))
            }
            .to[F]
        case None =>
          F.raiseError(
            KgError.InternalError(
              s"Error decoding key from S3 object URI '${fileMeta.location}' in bucket '${storage.bucket}'"))
      }
  }

  final class Save[F[_]](storage: S3Storage)(implicit F: Effect[F], as: ActorSystem, config: StorageConfig)
      extends SaveFile[F, AkkaSource] {

    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val mt: Materializer     = ActorMaterializer()

    private val attributes = S3Attributes.settings(storage.settings.toAlpakka(config.derivedKey))

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaSource): F[FileAttributes] = {
      val key            = mangle(storage.ref, fileDesc.uuid)
      val s3Sink         = S3.multipartUpload(storage.bucket, key).withAttributes(attributes)
      val metaDataSource = S3.getObjectMetadata(storage.bucket, key).withAttributes(attributes)

      val future = source
        .alsoToMat(digestSink(storage.algorithm))(Keep.right)
        .toMat(s3Sink) {
          case (digFuture, ioFuture) =>
            digFuture.zipWith(ioFuture) {
              case (dig, io) =>
                metaDataSource.runWith(Sink.head).flatMap {
                  case Some(meta) =>
                    Future.successful(fileDesc.process(StoredSummary(io.location, meta.contentLength, dig)))
                  case None =>
                    Future.failed(KgError.InternalError(
                      s"Empty content fetching metadata for uploaded file '${fileDesc.filename}' to location '${io.location}'"))
                }
              case _ =>
                Future.failed(KgError.InternalError(
                  s"I/O error uploading file with contentType '${fileDesc.mediaType}' and filename '${fileDesc.filename}'"))
            }
        }
        .run()
        .flatten

      IO.fromFuture(IO(future))
        .handleErrorWith {
          case e: KgError => IO.raiseError(e)
          case e: Throwable =>
            IO.raiseError(KgError.DownstreamServiceError(
              s"Error uploading S3 object with filename '${fileDesc.filename}' in bucket '${storage.bucket}': ${e.getMessage}"))
        }
        .to[F]
    }
  }

  final class Link[F[_]](storage: S3Storage)(implicit F: Effect[F], as: ActorSystem, config: StorageConfig)
      extends LinkFile[F] {

    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val mt: Materializer     = ActorMaterializer()

    override def apply(id: ResId, fileDesc: FileDescription, location: Uri): F[FileAttributes] =
      getKey(storage.bucket, location.path) match {
        case Some(key) =>
          val future =
            S3.download(storage.bucket, key)
              .withAttributes(S3Attributes.settings(storage.settings.toAlpakka(config.derivedKey)))
              .runWith(Sink.head)
              .flatMap {
                case Some((source, meta)) =>
                  source.runWith(digestSink(storage.algorithm)).map { dig =>
                    FileAttributes(location, fileDesc.filename, fileDesc.mediaType, meta.contentLength, dig)
                  }
                case None =>
                  Future.failed(
                    KgError.InternalError(
                      s"Empty content fetching S3 object with key '$key' in bucket '${storage.bucket}'"))
              }

          IO.fromFuture(IO(future))
            .handleErrorWith {
              case e: KgError => IO.raiseError(e)
              case e: Throwable =>
                IO.raiseError(
                  KgError.DownstreamServiceError(
                    s"Error fetching S3 object with key '$key' in bucket '${storage.bucket}': ${e.getMessage}"))
            }
            .to[F]
        case None =>
          F.raiseError(
            KgError.InternalError(s"Error decoding key from S3 object URI '$location' in bucket '${storage.bucket}'"))
      }
  }

  private def getKey(bucket: String, path: Path): Option[String] = path match {
    case Slash(Segment(head, Slash(tail))) if head == bucket =>
      Some(URLDecoder.decode(tail.toString, "UTF-8"))
    case _ =>
      logger.error(s"Error decoding key from S3 object URI '$path' in bucket '$bucket'")
      None
  }
}
