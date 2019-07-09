package ch.epfl.bluebrain.nexus.kg.routes

import cats.Id
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{resourceCtx, resourceCtxUri}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder
import ch.epfl.bluebrain.nexus.kg.resources.{Resource, ResourceV}
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.{Compacted, Expanded}
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.Json

object ResourceEncoder {

  private implicit val rootNodeResourceV: RootNode[ResourceV] = _.id.value
  private implicit val rootNodeResource: RootNode[Resource]   = _.id.value

  private implicit val resourceVGraphEnc: GraphEncoder[Id, ResourceV]                  = GraphEncoder((_, res) => res.value.graph)
  private implicit val resourceVGraphEncEither: GraphEncoder[DecoderResult, ResourceV] = resourceVGraphEnc.toEither

  private implicit def resourceGraphEnc(implicit config: AppConfig, project: Project): GraphEncoder[Id, Resource] =
    GraphEncoder((rootNode, res) => RootedGraph(rootNode, res.metadata()))
  private implicit def resourceGraphEncEither(implicit config: AppConfig,
                                              project: Project): GraphEncoder[DecoderResult, Resource] =
    resourceGraphEnc.toEither

  def json(r: Resource)(implicit config: AppConfig, project: Project): DecoderResult[Json] =
    r.as[Json](resourceCtx).map(_.replaceContext(resourceCtxUri))

  def json(res: ResourceV)(implicit output: JsonLDOutputFormat): DecoderResult[Json] =
    output match {
      case Compacted => jsonCompacted(res)
      case Expanded  => jsonExpanded(res)
    }

  private val resourceKeys: List[String] = resourceCtx.contextValue.asObject.map(_.keys.toList).getOrElse(List.empty)

  private def jsonCompacted(res: ResourceV): DecoderResult[Json] = {
    val flattenedContext = Json.obj("@context" -> res.value.ctx) mergeContext resourceCtx
    res.as[Json](flattenedContext).map { fieldsJson =>
      val contextJson =
        Json.obj("@context" -> res.value.source.contextValue.removeKeys(resourceKeys: _*)).addContext(resourceCtxUri)
      val json = fieldsJson deepMerge contextJson
      if (res.types.contains(nxv.ElasticSearchView.value)) ViewEncoder.transformToJson(json, nxv.mapping.prefix)
      else json
    }
  }

  private def jsonExpanded(r: ResourceV): DecoderResult[Json] =
    r.as[Json]().map { json =>
      if (r.types.contains(nxv.ElasticSearchView.value)) ViewEncoder.transformToJson(json, nxv.mapping.value.asString)
      else json
    }
}
