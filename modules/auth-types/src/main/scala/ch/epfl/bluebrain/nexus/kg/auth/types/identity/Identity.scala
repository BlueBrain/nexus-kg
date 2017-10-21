package ch.epfl.bluebrain.nexus.kg.auth.types.identity

import akka.http.scaladsl.model.Uri
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

import scala.util.Try

/**
  * Base enumeration type for identity classes.
  */
sealed trait Identity extends Product with Serializable

/**
  * Represents identities that were authenticated from a third party origin.
  */
trait Authenticated {

  /**
    * @return the third party domain from where the authentication origins
    */
  def origin: Uri
}

object Identity {

  /**
    * The ''user'' identity class.
    *
    * @param origin the authentication's origin
    * @param subject the JWT ''sub'' field
    */
  final case class UserRef(origin: Uri, subject: String) extends Identity with Authenticated

  /**
    * The ''group'' identity class.
    *
    * @param origin the authentication's origin
    * @param group the group name
    */
  final case class GroupRef(origin: Uri, group: String) extends Identity with Authenticated

  /**
    * The ''authenticated'' identity class that represents anyone authenticated from ''origin''.
    *
    * @param origin
    */
  final case class AuthenticatedRef(origin: Uri) extends Identity with Authenticated

  /**
    * The ''anonymous'' identity singleton that covers unknown and unauthenticated users.
    */
  final case object Anonymous extends Identity

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.emapTry(origin => Try(Uri(origin)))
  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.toString)

  implicit val identityDecoder: Decoder[Identity] = deriveDecoder[Identity]
  implicit val identityEncoder: Encoder[Identity] = deriveEncoder[Identity]

}
