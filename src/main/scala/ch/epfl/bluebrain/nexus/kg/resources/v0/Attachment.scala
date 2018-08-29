package ch.epfl.bluebrain.nexus.kg.resources.v0

import java.nio.file.Paths

import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

object Attachment {

  final case class Meta(fileUri: String, info: Info) {
    require(fileUri != null && !fileUri.isEmpty)

    def toBinaryAttributes =
      BinaryAttributes(Paths.get(fileUri), info.originalFileName, info.mediaType, info.contentSize, info.digest)
  }

  final case class Info(originalFileName: String, mediaType: String, contentSize: Size, digest: Digest)

  implicit val infoDecoder: Decoder[Info]    = deriveDecoder[Info]
  implicit val attMetaDecoder: Decoder[Meta] = deriveDecoder[Meta]
}
