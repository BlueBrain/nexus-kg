package ch.epfl.bluebrain.nexus.kg.service.io

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.Ref
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.linksEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys._
import io.circe.syntax._
import io.circe.{Encoder, Json}

/**
  * Constructs implicit encoders used to format HTTP responses.
  *
  * @param base      the service public uri + prefix
  * @param extractId the implicitly available extractor of an Id given an Entity
  * @param R         the implicitly available function which converts a Reference into a [[Ref]]
  * @param Q         the implicitly available qualifier for the generic type [[Id]]
  * @tparam Id        the generic type representing the id we want to encode
  * @tparam Reference the generic type representing the Ref we want to encode
  * @tparam Entity    the generic type representing the Entity we want to encode
  */
abstract class RoutesEncoder[Id, Reference, Entity](base: Uri, prefixes: PrefixUris)(implicit extractId: (Entity) => Id,
                                                                                     R: Reference => Ref[Id],
                                                                                     Q: Qualifier[Id])
    extends BaseEncoder(prefixes) {

  implicit val typeQualifier: ConfiguredQualifier[Id] = Qualifier.configured[Id](base)

  implicit val refEncoder: Encoder[Reference] = Encoder.encodeJson.contramap { ref =>
    Json.obj(
      `@id`  -> Json.fromString(ref.id.qualifyAsString),
      nxvRev -> Json.fromLong(ref.rev)
    )
  }

  implicit val idWithLinksEncoder: Encoder[Id] = Encoder.encodeJson.contramap { id =>
    Json.obj(
      `@id` -> Json.fromString(id.qualifyAsString),
      links -> selfLink(id).asJson
    )
  }

  implicit def queryResultEncoder(implicit E: Encoder[Id]): Encoder[UnscoredQueryResult[Id]] =
    Encoder.encodeJson.contramap { qr =>
      Json.obj(
        resultId -> Json.fromString(qr.source.qualifyAsString),
        source   -> E(qr.source)
      )
    }

  implicit def scoredQueryResultEncoder(implicit E: Encoder[Id]): Encoder[ScoredQueryResult[Id]] =
    Encoder.encodeJson.contramap { qr =>
      Json.obj(
        resultId -> Json.fromString(qr.source.qualifyAsString),
        score    -> Json.fromFloatOrString(qr.score),
        source   -> E(qr.source)
      )
    }

  implicit def queryResultEntityEncoder(implicit E: Encoder[Entity]): Encoder[UnscoredQueryResult[Entity]] =
    Encoder.encodeJson.contramap { qr =>
      Json.obj(
        resultId -> Json.fromString(extractId(qr.source).qualifyAsString),
        source   -> E(qr.source)
      )
    }

  implicit def scoredQueryResultEntityEncoder(implicit E: Encoder[Entity]): Encoder[ScoredQueryResult[Entity]] =
    Encoder.encodeJson.contramap { qr =>
      Json.obj(
        resultId -> Json.fromString(extractId(qr.source).qualifyAsString),
        score    -> Json.fromFloatOrString(qr.score),
        source   -> E(qr.source)
      )
    }

  private def selfLink(id: Id): Links = Links("self" -> id.qualify)

}

object RoutesEncoder {

  implicit val linksEncoder: Encoder[Links] =
    Encoder.encodeJson.contramap { links =>
      links.values.mapValues {
        case href :: Nil => Json.fromString(s"$href")
        case hrefs       => Json.arr(hrefs.map(href => Json.fromString(s"$href")): _*)
      }.asJson
    }

  object JsonLDKeys {
    val `@context`     = "@context"
    val `@id`          = "@id"
    val links          = "links"
    val resultId       = "resultId"
    val source         = "source"
    val score          = "score"
    val nxvNs          = "nxv"
    val nxvRev         = "nxv:rev"
    val nxvDeprecated  = "nxv:deprecated"
    val nxvDescription = "nxv:description"
    val nxvPublished   = "nxv:published"
  }

}
