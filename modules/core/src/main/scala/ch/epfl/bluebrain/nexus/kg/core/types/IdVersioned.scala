package ch.epfl.bluebrain.nexus.kg.core.types

import cats.instances.all._
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.RepresentationId
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

/**
  * A versioned reference of a resource's representation
  *
  * @param id  the reference to the resource
  * @param rev the selected revision of the resource
  */
final case class IdVersioned(id: RepresentationId, rev: Long)

object IdVersioned {
  implicit def idVersionedEncoder(implicit config: AdminConfig): Encoder[IdVersioned] = Encoder.instance {
    case IdVersioned(k, rev) => Json.obj("rev" -> Json.fromLong(rev)) deepMerge k.asJson
  }

  implicit def idVersionedDecoder(implicit config: AdminConfig): Decoder[IdVersioned] = Decoder.instance { hc =>
    (hc.value.as[RepresentationId], hc.get[Long]("rev")).mapN((key, rev) => IdVersioned(key, rev))
  }
}
