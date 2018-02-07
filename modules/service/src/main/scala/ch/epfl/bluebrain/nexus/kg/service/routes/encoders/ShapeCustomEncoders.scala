package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.{Shape, ShapeId, ShapeRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json}

class ShapeCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[ShapeId, ShapeRef, Shape](base, prefixes) {

  implicit val shapeRefEncoder: Encoder[ShapeRef] = refEncoder

  implicit def shapeEncoder: Encoder[Shape] = Encoder.encodeJson.contramap { shape =>
    val meta = refEncoder
      .apply(ShapeRef(shape.id, shape.rev))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(shape.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(shape.published)
        ))
    shape.value.deepMerge(meta)
  }
}
