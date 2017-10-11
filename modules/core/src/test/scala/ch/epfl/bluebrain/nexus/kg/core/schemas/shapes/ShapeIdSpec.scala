package ch.epfl.bluebrain.nexus.kg.core.schemas.shapes

import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class ShapeIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A ShapeId" should {
    val domId = DomainId(OrgId("org"), "dom")
    val schemaId = SchemaId(domId, "name", Version(1, 2, 3))
    val shapeId = ShapeId(schemaId, "ShapeFragment")
    val shapeIdString = """"org/dom/name/v1.2.3/shapes/ShapeFragment""""

    "be encoded properly into json" in {
      shapeId.asJson.noSpaces shouldEqual shapeIdString
    }

    "be decoded properly from json" in {
      decode[ShapeId](shapeIdString) shouldEqual Right(shapeId)
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
          "org/dom/name/v1.2.3/ShapeFragment 123",
          "org/dom/name/v1.2.3/http://google.com/ShapeFragment",
          "org/dom/name/v1.2.3/shapes/bbp:ShapeFragment"
        )) { str =>
        decode[ShapeId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
