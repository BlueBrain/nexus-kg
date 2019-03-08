package ch.epfl.bluebrain.nexus.kg.storage

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect.{Effect, IO}
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.resources.ResId
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription, StoredSummary}
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}

object S3StorageOperations {

  private val logger = Logger[this.type]

  final class Fetch(storage: S3Storage) extends FetchFile[AkkaSource] {

    override def apply(fileMeta: FileAttributes): AkkaSource = {
      val key = fileMeta.location.path.toString
      S3.download(storage.bucket, key)
        .withAttributes(S3Attributes.settings(storage.settings.toAlpakka))
        .flatMapConcat {
          case Some((source, _)) => source
          case None =>
            logger.error(s"Error fetching file '${fileMeta.filename}' from S3 bucket '${storage.bucket}'")
            Source.empty
        }
    }
  }

  final class Save[F[_]](storage: S3Storage)(implicit F: Effect[F], as: ActorSystem) extends SaveFile[F, AkkaSource] {

    private val attributes = S3Attributes.settings(storage.settings.toAlpakka)

    override def apply(id: ResId, fileDesc: FileDescription, source: AkkaSource): F[FileAttributes] = {
      implicit val ec: ExecutionContext = as.dispatcher
      implicit val mt: Materializer     = ActorMaterializer()

      val key            = mangle(storage.ref, fileDesc.uuid)
      val s3Sink         = S3.multipartUpload(storage.bucket, key).withAttributes(attributes)
      val metaDataSource = S3.getObjectMetadata(storage.bucket, key).withAttributes(attributes)

      val future = source
        .alsoToMat(digestSink(storage.algorithm))(Keep.right)
        .toMat(s3Sink) {
          case (digFuture, ioFuture) =>
            digFuture.zipWith(ioFuture.runWith(Sink.head)) {
              case (dig, io) =>
                val digest = Digest(dig.getAlgorithm, dig.digest.map("%02x".format(_)).mkString)
                metaDataSource.runWith(Sink.head).flatMap {
                  case Some(meta) =>
                    Future.successful(fileDesc.process(StoredSummary(io.location, meta.contentLength, digest)))
                  case None =>
                    Future.failed(
                      KgError.InternalError(s"I/O error fetching metadata for uploaded file '${io.location}'"))
                }
              case _ =>
                Future.failed(KgError.InternalError(
                  s"I/O error uploading file with contentType '${fileDesc.mediaType}' and filename '${fileDesc.filename}'"))
            }
        }
        .run()
        .flatten

      F.liftIO(IO.fromFuture(IO(future)))
    }
  }

}
