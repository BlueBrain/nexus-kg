package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class ContextIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A ContextId" should {
    val domId           = DomainId(OrgId("org"), "dom")
    val contextId       = ContextId(domId, "name", Version(1, 2, 3))
    val contextIdString = """"org/dom/name/v1.2.3""""

    "extract an contextName properly" in {
      contextId.contextName shouldEqual ContextName(domId, "name")
    }

    "be encoded properly into json" in {
      contextId.asJson.noSpaces shouldEqual contextIdString
    }

    "be decoded properly from json" in {
      decode[ContextId](contextIdString) shouldEqual Right(contextId)
    }

    "fail to decode" in {
      forAll(
        List("asd",
             "/",
             "/asd",
             "asd/",
             "asd/ads/asd",
             "asd/asd/asd/v1.1",
             "asd/asd/asd/v1.1.2.3",
             "asd/asd/asd/v1.1.a",
             "asd/asd/a d/v1.1.2")) { str =>
        decode[ContextId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
