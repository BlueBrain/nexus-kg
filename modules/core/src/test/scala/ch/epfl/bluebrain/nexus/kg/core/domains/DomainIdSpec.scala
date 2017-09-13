package ch.epfl.bluebrain.nexus.kg.core.domains

import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class DomainIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A DomainId" should {
    "be encoded properly into json" in {
      DomainId(OrgId("org"), "dom").asJson.noSpaces shouldEqual """"org/dom""""
    }

    "be decoded properly from json" in {
      decode[DomainId](""""org/dom"""") shouldEqual Right(DomainId(OrgId("org"), "dom"))
    }

    "fail to decode" in {
      forAll(List("asd", "/", "/asd", "asd/", "asd/ads/asd")) { str =>
        decode[DomainId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}