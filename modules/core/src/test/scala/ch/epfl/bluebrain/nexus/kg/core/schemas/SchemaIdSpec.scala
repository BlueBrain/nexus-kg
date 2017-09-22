package ch.epfl.bluebrain.nexus.kg.core.schemas

import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class SchemaIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A SchemaId" should {
    val domId = DomainId(OrgId("org"), "dom")
    val schemaId = SchemaId(domId, "name", Version(1, 2, 3))
    val schemaIdString = """"org/dom/name/v1.2.3""""

    "extract an schemaName properly" in {
      schemaId.schemaName shouldEqual SchemaName(domId, "name")
    }

    "be encoded properly into json" in {
      schemaId.asJson.noSpaces shouldEqual schemaIdString
    }

    "be decoded properly from json" in {
      decode[SchemaId](schemaIdString) shouldEqual Right(schemaId)
    }

    "fail to decode" in {
      forAll(List(
        "asd",
        "/",
        "/asd",
        "asd/",
        "asd/ads/asd",
        "asd/asd/asd/v1.1",
        "asd/asd/asd/v1.1.2.3",
        "asd/asd/asd/v1.1.a",
        "asd/asd/a d/v1.1.2")) { str =>
        decode[SchemaId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}