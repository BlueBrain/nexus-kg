package ch.epfl.bluebrain.nexus.kg.auth.types

import ch.epfl.bluebrain.nexus.kg.auth.types.Permission.valid
import io.circe._

/**
  * Wraps a permission string, e.g. ''own'', ''read'', ''write''.
  *
  * @param value a valid permission string
  * @throws IllegalArgumentException
  */
final case class Permission(value: String) {
  require(value.matches(valid.regex), "Permission string must be 1 to 16 lowercase letters")
}

object Permission {
  private[types] val valid = "[a-z]{1,16}".r

  /**
    * Resource ownership access permission definition. Owning a resource offers the ability
    * to change the ownership group and set permissions on all resources and sub-resources.
    */
  val Own = Permission("own")

  /**
    * Resource read access permission definition. Read access to a resource allows
    * viewing the current state and history of the resource.
    */
  val Read = Permission("read")

  /**
    * Resource write access permission definition. Write access to a resource allows
    * changing the current state of the resource.
    */
  val Write = Permission("write")

  implicit val permissionKeyEncoder: KeyEncoder[Permission] = KeyEncoder.encodeKeyString.contramap(_.value)

  implicit val permissionKeyDecoder: KeyDecoder[Permission] = KeyDecoder.instance {
    case str @ valid() => Some(Permission(str))
    case _             => None
  }

  implicit val permissionEncoder: Encoder[Permission] = Encoder.encodeString.contramap[Permission](_.value)

  implicit val permissionDecoder: Decoder[Permission] = Decoder.decodeString.emap {
    case str @ valid() => Right(Permission(str))
    case _             => Left("Illegal permission format")
  }
}
