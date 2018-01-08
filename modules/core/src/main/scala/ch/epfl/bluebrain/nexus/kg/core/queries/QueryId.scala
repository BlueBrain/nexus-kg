package ch.epfl.bluebrain.nexus.kg.core.queries

import cats.Show
import io.circe.{Decoder, Encoder}

/**
  * Unique query identifier
  *
  * @param id the unique query identifier
  */
final case class QueryId(id: String)

object QueryId {

  final implicit def queryIdShow: Show[QueryId] =
    Show.show(id => s"${id.id}")

  final implicit def queryIdIdEncoder(implicit S: Show[QueryId]): Encoder[QueryId] =
    Encoder.encodeString.contramap(id => S.show(id))

  final implicit val queryIdDecoder: Decoder[QueryId] =
    Decoder.decodeString.map(str => QueryId(str))

}
