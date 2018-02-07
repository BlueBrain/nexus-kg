package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json}

class DomainCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[DomainId, DomainRef, Domain](base, prefixes) {

  implicit val domainRefEncoder: Encoder[DomainRef] = refEncoder

  implicit def domainEncoder: Encoder[Domain] = Encoder.encodeJson.contramap { domain =>
    refEncoder
      .apply(DomainRef(domain.id, domain.rev))
      .deepMerge(idWithLinksEncoder(domain.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated  -> Json.fromBoolean(domain.deprecated),
          JsonLDKeys.nxvDescription -> Json.fromString(domain.description)
        ))
  }
}
