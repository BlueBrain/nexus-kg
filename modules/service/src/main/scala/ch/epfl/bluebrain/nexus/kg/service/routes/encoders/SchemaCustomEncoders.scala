package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json}

class SchemaCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[SchemaId, SchemaRef, Schema](base, prefixes) {

  implicit val schemaRefEncoder: Encoder[SchemaRef] = refEncoder

  implicit def schemaEncoder: Encoder[Schema] = Encoder.encodeJson.contramap { schema =>
    val meta = refEncoder
      .apply(SchemaRef(schema.id, schema.rev))
      .deepMerge(idWithLinksEncoder(schema.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(schema.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(schema.published)
        ))
    schema.value.deepMerge(meta)
  }
}
