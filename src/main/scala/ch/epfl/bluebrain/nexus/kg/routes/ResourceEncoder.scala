package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtx, resourceCtxUri}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, ResourceV}
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.{Compacted, Expanded}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import io.circe.{Encoder, Json}

object ResourceEncoder {

  private implicit val resourceVGraphEnc: GraphEncoder[ResourceV] = GraphEncoder(
    res => IriNode(res.id.value) -> res.value.graph)

  implicit def resourceEncoder(implicit config: AppConfig, project: Project): Encoder[Resource] = {
    implicit val graphEnc: GraphEncoder[Resource] =
      GraphEncoder(res => IriNode(res.id.value) -> Graph(res.metadata))
    Encoder.encodeJson.contramap { res =>
      res.asJson(resourceCtx).removeKeys("@context").addContext(resourceCtxUri)
    }
  }

  implicit def resourceVEncoder(implicit output: JsonLDOutputFormat): Encoder[ResourceV] =
    output match {
      case Compacted => resourceVEncoderCompacted
      case Expanded  => resourceVEncoderExpanded
    }

  private def resourceVEncoderCompacted: Encoder[ResourceV] =
    Encoder.encodeJson.contramap { res =>
      val flattenedContext = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
      val fieldsJson       = res.asJson(flattenedContext)
      val contextJson      = Json.obj("@context" -> res.contextValueForJsonLd).addContext(resourceCtxUri)
      val json             = fieldsJson deepMerge contextJson
      if (res.types.contains(nxv.ElasticSearchView.value)) ViewEncoder.transformToJson(json, nxv.mapping.prefix)
      else json
    }

  private val resourceVEncoderExpanded: Encoder[ResourceV] =
    Encoder.encodeJson.contramap { res =>
      val json = res.asExpandedJson
      if (res.types.contains(nxv.ElasticSearchView.value)) ViewEncoder.transformToJson(json, nxv.mapping.value.asString)
      else json
    }
}
