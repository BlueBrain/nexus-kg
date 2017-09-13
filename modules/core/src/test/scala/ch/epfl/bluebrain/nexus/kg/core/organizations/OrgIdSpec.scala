package ch.epfl.bluebrain.nexus.kg.core.organizations

import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import scala.util.Left
import OrgId._

class OrgIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A OrgId" should {
    "be encoded properly into json" in {
      OrgId("org").asJson.noSpaces shouldEqual """"org""""
    }

    "be decoded properly from json" in {
      decode[OrgId](""""org"""") shouldEqual Right(OrgId("org"))
    }

    "fail to decode" in {
      forAll(List("asd!", "/", "/asd", "asd/", "asd/ads/asd")) { str =>
        decode[OrgId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}