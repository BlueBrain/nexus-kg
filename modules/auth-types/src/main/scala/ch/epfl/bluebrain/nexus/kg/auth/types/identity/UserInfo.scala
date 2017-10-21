package ch.epfl.bluebrain.nexus.kg.auth.types.identity

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

/**
  * Detailed user information.
  *
  * @param sub               the subject (typically corresponds to the user id)
  * @param name              the name of the user
  * @param preferredUsername the preferred user name (used for login purposes)
  * @param givenName         the given name
  * @param familyName        the family name
  * @param email             the email
  * @param groups            the collection of groups that this user belongs to
  */
final case class UserInfo(sub: String,
                          name: String,
                          preferredUsername: String,
                          givenName: String,
                          familyName: String,
                          email: String,
                          groups: Set[String]) {}

object UserInfo {

  implicit val config: Configuration              = Configuration.default.withSnakeCaseKeys
  implicit val userInfoDecoder: Decoder[UserInfo] = deriveDecoder[UserInfo]
  implicit val userInfoEncoder: Encoder[UserInfo] = deriveEncoder[UserInfo]
}
