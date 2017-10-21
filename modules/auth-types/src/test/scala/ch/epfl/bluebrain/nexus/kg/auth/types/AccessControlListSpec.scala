package ch.epfl.bluebrain.nexus.kg.auth.types

import ch.epfl.bluebrain.nexus.kg.auth.types.Permission.{Own, Read, Write}
import ch.epfl.bluebrain.nexus.kg.auth.types.identity.Identity.GroupRef
import io.circe.generic.extras.auto._
import io.circe.generic.extras.Configuration
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Matchers, WordSpecLike}

class AccessControlListSpec extends WordSpecLike with Matchers {

  "A AccessControlList" should {
    val permissions                    = Set(Own, Read, Write, Permission("publish"))
    implicit val config: Configuration = Configuration.default.withDiscriminator("type")
    val model = AccessControlList(
      Set(AccessControl(GroupRef("https://bbpteam.epfl.ch/auth/realms/BBP", "/bbp-ou-nexus"), permissions)))
    val json =
      """{"acl":[{"identity":{"origin":"https://bbpteam.epfl.ch/auth/realms/BBP","group":"/bbp-ou-nexus","type":"GroupRef"},"permissions":["own","read","write","publish"]}]}"""

    "be decoded from Json properly" in {
      decode[AccessControlList](json) shouldEqual Right(model)
    }
    "be encoded to Json properly" in {
      model.asJson.noSpaces shouldEqual json
    }
  }

}
