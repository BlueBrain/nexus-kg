package ch.epfl.bluebrain.nexus.kg.core.circe

import ch.epfl.bluebrain.nexus.common.test.Resources
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.core.circe.CirceShapeExtractorInstances._
import io.circe.Json

class CirceShapeExtractorSpec extends WordSpecLike
  with Matchers
  with Inspectors
  with Resources {

  val schemaJson = jsonContentOf("/int-value-schema.json")
  val shapeNodeShape = jsonContentOf("/int-value-shape-nodeshape.json")
  val shapeSomething = jsonContentOf("/int-value-shape-something.json")
  val schemaNoShapes = Json.obj("one" -> Json.fromString("two"))

  "A CirceSchemaExtractor" should {

    "fetch the desired shape from a schema" in {
      val values = Map[String, Option[Json]](
        "IdSomething" -> Some(shapeSomething),
        "IdNodeShape2" -> Some(shapeNodeShape),
        "bbp:IdSomething" -> None,
        "Something" -> None,
        ":IdSomething" -> None,
        "/IdNodeShape2" -> None,
        "dNodeShape2" -> None)

      forAll(values.toList) { case (id, expected) =>
        schemaJson.fetchShape(id) shouldEqual expected
      }
      schemaNoShapes.fetchShape("Something") shouldEqual None
    }
  }
}
