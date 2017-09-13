package ch.epfl.bluebrain.nexus.kg.core.schemas.shapes

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.{Decoder, Encoder}

import scala.util.{Failure, Try}
import scala.util.matching.Regex

/**
  * Unique schema identifier rooted in a domain.
  *
  * @param schemaId the schema identifier for this shape
  * @param name     the name of the shape
  */
final case class ShapeId(schemaId: SchemaId, name: String)

object ShapeId {
  final val regex: Regex = s"""${SchemaId.regex.regex}/shapes/([a-zA-Z0-9]+)""".r

  final implicit def shapeIdShow(implicit S: Show[SchemaId]): Show[ShapeId] = Show.show { id =>
    s"${id.schemaId.show}/shapes/${id.name}"
  }

  final implicit def shapeIdEnconder(implicit S: Show[ShapeId]): Encoder[ShapeId] =
    Encoder.encodeString.contramap(id => S.show(id))

  final implicit val shapeIdDecoder: Decoder[ShapeId] =
    Decoder.decodeString.emapTry {
      case regex(org, dom, name, major, minor, patch, fragment) =>
        Try(Version(major.toInt, minor.toInt, patch.toInt)).map { ver =>
          ShapeId(SchemaId(DomainId(OrgId(org), dom), name, ver), fragment)
        }
      case _                                                    =>
        Failure(new IllegalArgumentException("Unable to decode value into a ShapeId"))
    }
}
