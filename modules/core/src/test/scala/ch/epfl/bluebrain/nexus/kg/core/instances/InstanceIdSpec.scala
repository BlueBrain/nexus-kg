package ch.epfl.bluebrain.nexus.kg.core.instances

import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class InstanceIdSpec extends WordSpecLike with Matchers with Inspectors {

  "An InstanceId" should {
    val domId    = DomainId(OrgId("org"), "dom")
    val schemaId = SchemaId(domId, "name", Version(1, 2, 3))
    val id       = InstanceId(schemaId, "f9a9e240-4763-430e-a25c-0837dd55ffdb")
    val idString =
      """"org/dom/name/v1.2.3/f9a9e240-4763-430e-a25c-0837dd55ffdb""""

    "be encoded properly into json" in {
      id.asJson.noSpaces shouldEqual idString
    }

    "be decoded properly from json" in {
      decode[InstanceId](idString) shouldEqual Right(id)
    }

    "fail to decode" in {
      forAll(
        List(
          "asd",
          "/",
          "/asd",
          "asd/",
          "asd/ads/asd",
          "asd/asd/asd/v1.1",
          "asd/asd/asd/v1.1.2.3",
          "asd/asd/asd/v1.1.a",
          "asd/asd/a d/v1.1.2",
          "asd/asd/a d/v1.1.2/",
          "asd/asd/a d/v1.1.2//",
          "asd/asd/a d/v1.1.2/f9a9e240-4763-430e-a25c-0837dd55ffd",
          "asd/asd/a d/v1.1.2/f9a9e240-4763-430e-25c-0837dd55ffdb",
          "asd/asd/a d/v1.1.2/f9a9e240-4763-430e-a25c-0837dd55ffdb/a"
        )) { str =>
        decode[SchemaId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
