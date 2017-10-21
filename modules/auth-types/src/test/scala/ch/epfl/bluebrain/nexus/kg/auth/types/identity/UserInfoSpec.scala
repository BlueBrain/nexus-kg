package ch.epfl.bluebrain.nexus.kg.auth.types.identity

import ch.epfl.bluebrain.nexus.kg.auth.types.identity.Identity._
import io.circe.DecodingFailure
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest._
import ch.epfl.bluebrain.nexus.kg.auth.types.identity.UserInfo._
import scala.util._

class UserInfoSpec extends WordSpecLike with Matchers with Inspectors {

  private val json =
    s"""{"sub":"sub","name":"name","preferred_username":"preferredUsername","given_name":"givenName","family_name":"familyName","email":"email@example.com","groups":["group1","group2"]}"""

  private val model = UserInfo("sub",
                               "name",
                               "preferredUsername",
                               "givenName",
                               "familyName",
                               "email@example.com",
                               Set("group1", "group2"))

  "A UserInfo" should {
    "be decoded from Json properly" in {
      decode[UserInfo](json) shouldEqual Right(model)
    }
    "be encoded to Json properly" in {
      model.asJson.noSpaces shouldEqual json
    }
    "not be decoded when origin URI is bogus" in {
      decode[Identity]("""{"sub":1}""") match {
        case Right(_) => fail()
        case Left(e)  => e shouldBe a[DecodingFailure]
      }
    }
  }
}
