package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtx, resourceCtxUri}
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import io.circe.{Encoder, Json}

object ResourceEncoder {
  private implicit def graphEncoderResourceF(implicit iamConfig: IamConfig): GraphEncoder[Resource] = GraphEncoder {
    res =>
      IriNode(res.id.value) -> (res.metadata ++ res.typeGraph)
  }

  private implicit def graphEncoderResourceV: GraphEncoder[ResourceV] = GraphEncoder { res =>
    IriNode(res.id.value) -> res.value.graph
  }

  implicit def resourceEncoder(implicit iamConfig: IamConfig): Encoder[Resource] = Encoder.encodeJson.contramap { res =>
    res.asJson(resourceCtx).removeKeys("@context").addContext(resourceCtxUri)
  }

  implicit def resourceVEncoder: Encoder[ResourceV] = Encoder.encodeJson.contramap { res =>
    val mergedCtx = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
    res.asJson(mergedCtx) deepMerge Json.obj("@context" -> res.value.source.contextValue).addContext(resourceCtxUri)
  }
}
