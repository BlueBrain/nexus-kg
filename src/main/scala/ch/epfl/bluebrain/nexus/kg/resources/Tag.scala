package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.{Encoder, Json}

/**
  * Represents a tag
  * @param rev the tag revision
  * @param value the tag name
  */
final case class Tag(rev: Long, value: String)

object Tag {

  /**
    * Attempts to constructs a Tag from the provided json
    *
    * @param resId the resource identifier
    * @param json  the payload
    * @return Right(tag) when successful and Left(rejection) when failed
    */
  final def apply(resId: ResId, json: Json): Either[Rejection, Tag] =
    // format: off
    for {
      graph   <- (json deepMerge tagCtx).id(resId.value).asGraph(resId.value)
                    .left.map(_ => InvalidResourceFormat(resId.ref, "Empty or wrong Json-LD. Both 'tag' and 'rev' fields must be present."))
      cursor   = graph.cursor()
      rev     <- cursor.downField(nxv.rev).focus.as[Long].left.map(_ => InvalidResourceFormat(resId.ref, "'rev' field does not have the right format."))
      tag     <- cursor.downField(nxv.tag).focus.as[String].flatMap(nonEmpty).left.map(_ => InvalidResourceFormat(resId.ref, "'tag' field does not have the right format."))
    } yield Tag(rev, tag)
  // format: on

  implicit val tagEncoder: Encoder[Tag] = Encoder.instance {
    case Tag(rev, tag) => Json.obj(nxv.tag.prefix -> Json.fromString(tag), "rev" -> Json.fromLong(rev))
  }
}
