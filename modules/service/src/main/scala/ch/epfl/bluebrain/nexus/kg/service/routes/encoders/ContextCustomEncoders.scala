package ch.epfl.bluebrain.nexus.kg.service.routes.encoders

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.{OrderedKeys, jsonLdMarshaller}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.EntityToIdRetrieval._
import io.circe.{Encoder, Json, Printer}

class ContextCustomEncoders(base: Uri, prefixes: PrefixUris)
    extends RoutesEncoder[ContextId, ContextRef, Context](base, prefixes) {

  implicit val contextRefEncoder: Encoder[ContextRef] = refEncoder.mapJson(_.addCoreContext)

  implicit val contextEncoder: Encoder[Context] = Encoder.encodeJson.contramap { ctx =>
    val meta = refEncoder
      .apply(ContextRef(ctx.id, ctx.rev))
      .deepMerge(idWithLinksEncoder(ctx.id))
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
