package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.{OrderedKeys, jsonLdMarshaller}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.{JsonLDKeys, linksEncoder}
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json, Printer}

class ContextCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[ContextId, ContextRef, Context](base, prefixes) {

  private val contextsLinkContext = Json.obj(
    JsonLDKeys.`@context` -> Json.obj(
      "self" -> Json.obj(
        JsonLDKeys.`@id` -> Json.fromString("nxv:self"),
        "@type"          -> Json.fromString(JsonLDKeys.`@id`)
      )
    )
  )
  implicit val linksWithContextForContextsEncoder: Encoder[Links] =
    linksEncoder.mapJson(_ deepMerge contextsLinkContext)

  implicit val idWithLinksForContextsEncoder: Encoder[ContextId] = Encoder.encodeJson.contramap { id =>
    Json.obj(
      JsonLDKeys.`@id` -> Json.fromString(id.qualifyAsString),
      JsonLDKeys.links -> linksWithContextForContextsEncoder(selfLink(id))
    )
  }

  implicit val contextRefEncoder: Encoder[ContextRef] = refEncoder.mapJson(_.addCoreContext)

  implicit val contextEncoder: Encoder[Context] = Encoder.encodeJson.contramap { ctx =>
    val meta = refEncoder
      .apply(ContextRef(ctx.id, ctx.rev))
      .deepMerge(idWithLinksForContextsEncoder(ctx.id))
      .deepMerge(
        Json.obj(
          JsonLDKeys.nxvDeprecated -> Json.fromBoolean(ctx.deprecated),
          JsonLDKeys.nxvPublished  -> Json.fromBoolean(ctx.published)
        )
      )
    if (ctx.id.qualify == prefixes.CoreContext.context)
      ctx.value.deepMerge(meta)
    else
      ctx.value.deepMerge(meta).addCoreContext
  }

  /**
    * Custom marshaller that doesn't inject the context into the JSON-LD payload.
    *
    * @return a [[Context]] marshaller
    */
  implicit def marshallerHttp(implicit
                              encoder: Encoder[Context],
                              printer: Printer = Printer.noSpaces.copy(dropNullValues = true),
                              keys: OrderedKeys = OrderedKeys()): ToEntityMarshaller[Context] =
    jsonLdMarshaller.compose(encoder.apply)
}
