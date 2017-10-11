package ch.epfl.bluebrain.nexus.kg.core.instances

import cats.Show
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.{Decoder, Encoder}

import scala.util.matching.Regex
import scala.util.{Failure, Try}

/**
  * Unique instance identifier rooted in a schema.
  *
  * @param schemaId the schema identifier for this instance
  * @param id       the unique instance identifier
  */
final case class InstanceId(schemaId: SchemaId, id: String)

object InstanceId {
  final val regex: Regex =
    s"""${SchemaId.regex.regex}/([a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12})""".r

  /**
    * Attempts to parse the argument string into an ''InstanceId''.
    *
    * @param string the string representation of the instance id.
    * @return either a ''Success[InstanceId]'' or a ''Failure'' wrapping an [[IllegalArgumentException]]
    */
  final def apply(string: String): Try[InstanceId] = string match {
    case regex(org, dom, name, major, minor, patch, id) =>
      Try(Version(major.toInt, minor.toInt, patch.toInt)).map { ver =>
        InstanceId(SchemaId(DomainId(OrgId(org), dom), name, ver), id)
      }
    case _ =>
      Failure(new IllegalArgumentException("Unable to decode value into an InstanceId"))
  }

  final implicit def instanceIdShow(implicit S: Show[SchemaId]): Show[InstanceId] =
    Show.show(id => s"${S.show(id.schemaId)}/${id.id}")

  final implicit def instanceIdEncoder(implicit S: Show[InstanceId]): Encoder[InstanceId] =
    Encoder.encodeString.contramap(id => S.show(id))

  final implicit val instanceIdDecoder: Decoder[InstanceId] =
    Decoder.decodeString.emapTry(str => apply(str))

}
