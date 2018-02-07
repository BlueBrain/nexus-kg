package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json}

class OrgCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[OrgId, OrgRef, Organization](base, prefixes) {

  implicit val orgRefEncoder: Encoder[OrgRef] = refEncoder

  implicit val orgEncoder: Encoder[Organization] = Encoder.encodeJson.contramap { org =>
    val meta = refEncoder
      .apply(OrgRef(org.id, org.rev))
      .deepMerge(idWithLinksEncoder(org.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(org.deprecated)
        ))
    org.value.deepMerge(meta)
  }
}
