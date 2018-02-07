package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment.downloadUrlKey
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}

class InstanceCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[InstanceId, InstanceRef, Instance](base, prefixes) {

  implicit val qualifierSchema: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)

  implicit val instanceEncoder: Encoder[Instance] = Encoder.encodeJson.contramap { instance =>
    val instanceRef = InstanceRef(instance.id, instance.rev, instance.attachment)
    val downloadURL = Json.obj("downloadURL" -> Json.fromString(s"${instance.id.qualifyAsString}/attachment"))
    val ours        = instance.attachment.map(_.asJson.addDistributionContext deepMerge downloadURL).toList
    val theirs      = instance.value.asObject.flatMap(_.apply(JsonLDKeys.distribution)).toList
    val merged      = ours ++ theirs

    val meta = refEncoder
      .apply(instanceRef)
      .deepMerge(instanceIdWithLinksEncoder(instance.id))
      .deepMerge(if (merged.isEmpty) {
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(instance.deprecated)
        )
      } else {
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(instance.deprecated),
          JsonLDKeys.distribution  -> Json.fromValues(merged)
        )
      })

    instance.value.deepMerge(meta)
  }

  implicit val instanceRefEncoder: Encoder[InstanceRef] = Encoder.encodeJson.contramap { ref =>
    ref.attachment match {
      case Some(attachment) =>
        val downloadURL = Json.obj(downloadUrlKey -> Json.fromString(s"${ref.id.qualifyAsString}/attachment"))
        refEncoder
          .apply(ref)
          .deepMerge(Json.obj(JsonLDKeys.distribution -> Json.arr(attachment.asJson.addDistributionContext deepMerge downloadURL)))

      case None =>
        refEncoder
          .apply(ref)
    }
  }

  implicit val instanceIdWithLinksEncoder: Encoder[InstanceId] = Encoder.encodeJson.contramap { instanceId =>
    val linksJson = Links(
      "self"     -> instanceId.qualify,
      "schema"   -> instanceId.schemaId.qualify,
      "outgoing" -> s"${instanceId.qualifyAsString}/outgoing",
      "incoming" -> s"${instanceId.qualifyAsString}/incoming"
    ).asJson
    idWithLinksEncoder.apply(instanceId) deepMerge Json.obj("links" -> linksJson)
  }

}
