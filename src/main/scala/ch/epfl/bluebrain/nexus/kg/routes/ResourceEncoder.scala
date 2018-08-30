package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtx, resourceCtxUri}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import io.circe.{Encoder, Json}

object ResourceEncoder {

  private implicit def graphEncoderResourceV(implicit http: HttpConfig,
                                             wrapped: LabeledProject): GraphEncoder[ResourceV] =
    GraphEncoder { res =>
      val id = IriNode(res.id.value)
      def triplesFor(at: BinaryAttributes): Set[Triple] = {
        val blank       = Node.blank
        val blankSize   = Node.blank
        val blankDigest = Node.blank
        Set(
          (blankSize, nxv.unit, at.contentSize.unit),
          (blankSize, nxv.value, at.contentSize.value),
          (blankDigest, nxv.algorithm, at.digest.algorithm),
          (blankDigest, nxv.value, at.digest.value),
          (blank, nxv.contentSize, blankSize),
          (blank, nxv.digest, blankDigest),
          (blank, nxv.mediaType, at.mediaType),
          (blank, nxv.originalFileName, at.filename),
          (blank, nxv.downloadURL, res.accessId + "attachments" + at.filename),
          (id, schema.distribution, blank)
        )
      }
      id -> (res.value.graph ++ Graph(res.attachments.flatMap(triplesFor)))
    }

  implicit def resourceEncoder(implicit config: AppConfig, wrapped: LabeledProject): Encoder[Resource] = {
    implicit def encoderGraph: GraphEncoder[Resource] = GraphEncoder { res =>
      IriNode(res.id.value) -> (res.metadata ++ res.typeGraph)
    }

    Encoder.encodeJson.contramap { res =>
      res.asJson(resourceCtx).removeKeys("@context").addContext(resourceCtxUri)
    }
  }

  implicit def resourceVEncoder(implicit http: HttpConfig, wrapped: LabeledProject): Encoder[ResourceV] =
    Encoder.encodeJson.contramap { res =>
      val mergedCtx = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
      val json = res.asJson(mergedCtx) deepMerge Json
        .obj("@context" -> res.value.source.contextValue)
        .addContext(resourceCtxUri)
      if (res.types.contains(nxv.ElasticView.value)) ViewEncoder.transformToJson(json) else json
    }
}
