package ch.epfl.bluebrain.nexus.kg

import java.security.MessageDigest

import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

package object storage {
  type AkkaSource = Source[ByteString, Any]

  /**
    * A sink that computes the digest of the ByteString
    * @param algorithm the digest algorithm. E.g.: SHA-256
    */
  def digestSink(algorithm: String): Sink[ByteString, Future[MessageDigest]] =
    Sink.fold(MessageDigest.getInstance(algorithm))((digest, currentBytes) => {
      digest.update(currentBytes.asByteBuffer)
      digest
    })
}
