package ch.epfl.bluebrain.nexus.kg.service.query

import ch.epfl.bluebrain.nexus.commons.types.search.{Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.contexts.JenaExpander.expand
import ch.epfl.bluebrain.nexus.kg.core.queries.JsonLdFormat._
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryResource._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.core.queries.{Field, JsonLdFormat, QueryResource}
import io.circe.{Decoder, Json}

import scala.concurrent.Future

/**
  * Provides the decoder for [[QueryPayload]] and all the composed decoders.
  *
  * @param ctx the context applied for [[QueryPayload]]
  */
class QueryPayloadDecoder(ctx: Json)(implicit fs: FilteringSettings) {

  implicit val filterDec = Filter.filterDecoder(ctx)

  implicit val fieldDecoder: Decoder[Field] =
    Decoder.decodeString.map(field => Field(expand(field, ctx)))

  implicit val fieldSetDecoder: Decoder[Set[Field]] =
    Decoder.decodeSet[Field].map(set => set.filterNot(_ == Field.Empty))

  implicit val sortDecoder: Decoder[Sort] =
    Decoder.decodeString.map(Sort(_)).map { case Sort(order, value) => Sort(order, expand(value, ctx)) }

  implicit val sortlistDecoder: Decoder[SortList] =
    Decoder.decodeList[Sort].map(list => SortList(list))

  final implicit val decodeQuery: Decoder[QueryPayload] =
    Decoder.instance { hc =>
      for {
        filter     <- hc.downField("filter").as[Option[Filter]].map(_ getOrElse Filter.Empty)
        q          <- hc.downField("q").as[Option[String]].map(_ orElse None)
        deprecated <- hc.downField("deprecated").as[Option[Boolean]].map(_ orElse None)
        published  <- hc.downField("published").as[Option[Boolean]].map(_ orElse None)
        format     <- hc.downField("format").as[Option[JsonLdFormat]].map(_ getOrElse JsonLdFormat.Default)
        resource   <- hc.downField("resource").as[Option[QueryResource]].map(_ getOrElse QueryResource.Instances)
        fields     <- hc.downField("fields").as[Option[Set[Field]]].map(_ getOrElse Set.empty[Field])
        sort       <- hc.downField("sort").as[Option[SortList]].map(_ getOrElse SortList.Empty)

      } yield (QueryPayload(ctx, filter, q, deprecated, published, format, resource, fields, sort))
    }
}

object QueryPayloadDecoder {

  /**
    * Constrcuts a [[QueryPayloadDecoder]]
    *
    * @param ctx the provided context
    */
  final def apply(ctx: Json)(implicit fs: FilteringSettings): QueryPayloadDecoder = new QueryPayloadDecoder(ctx)

  /**
    * Resolves the context merging the @context inside the provided ''json'' with the context in ''fs''
    *
    * @param json the json which may contain a ''@context'' value
    */
  final def resolveContext(json: Json)(implicit fs: FilteringSettings, ctxs: Contexts[Future]): Future[Json] = {
    val context = json.hcursor.get[Json]("@context").getOrElse(Json.obj())
    ctxs.resolve(context.deepMerge(fs.ctx))
  }

}
