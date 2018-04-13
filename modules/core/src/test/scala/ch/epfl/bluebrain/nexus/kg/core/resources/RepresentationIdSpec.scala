package ch.epfl.bluebrain.nexus.kg.core.resources

import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class RepresentationIdSpec extends WordSpecLike with Matchers with Inspectors with EitherValues {

  private implicit val config = AdminConfig("http://localhost:8080/admin", "projects")
  "A RepresentationId" should {
    val model = RepresentationId("project", "some/id", "some/schema")
    val json = Json.obj("project" -> Json.fromString("http://localhost:8080/admin/projects/project"),
                        "@id"    -> Json.fromString("some/id"),
                        "schema" -> Json.fromString("some/schema"))

    "encode known events " in {
      model.asJson shouldEqual json

    }

    "decode known events" in {
      json.as[RepresentationId].right.value shouldEqual model
    }
  }

}
