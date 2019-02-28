package ch.epfl.bluebrain.nexus.kg

import java.security.MessageDigest

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

package object storage {
  type AkkaIn = Source[ByteString, Any]
  // TODO: Change the Materialization type to something like Future[Any], Future[NotUsed], Any or NotUsed
  type AkkaOut = Source[ByteString, Future[IOResult]]

  def digestSink(algorithm: String): Sink[ByteString, Future[MessageDigest]] =
    Sink.fold(MessageDigest.getInstance(algorithm))((digest, currentBytes) => {
      digest.update(currentBytes.asByteBuffer)
      digest
    })
}
