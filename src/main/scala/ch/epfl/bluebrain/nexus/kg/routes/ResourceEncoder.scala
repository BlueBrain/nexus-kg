package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtx, resourceCtxUri}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import io.circe.{Encoder, Json}

object ResourceEncoder {

  implicit def resourceEncoder(implicit config: AppConfig, project: Project): Encoder[Resource] = {
    implicit val graphEnc: GraphEncoder[Resource] =
      GraphEncoder(res => IriNode(res.id.value) -> Graph(res.metadata))
    Encoder.encodeJson.contramap { res =>
      res.asJson(resourceCtx).removeKeys("@context").addContext(resourceCtxUri)
    }
  }

  implicit val resourceVEncoder: Encoder[ResourceV] = {
    implicit val graphEnc: GraphEncoder[ResourceV] = GraphEncoder(res => IriNode(res.id.value) -> res.value.graph)
    Encoder.encodeJson.contramap { res =>
      val mergedCtx = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
      val json = res.asJson(mergedCtx) deepMerge Json
        .obj("@context" -> res.value.ctx)
        .addContext(resourceCtxUri)
      if (res.types.contains(nxv.ElasticView.value)) ViewEncoder.transformToJson(json) else json
    }
  }
}
