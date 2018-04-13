package ch.epfl.bluebrain.nexus.kg.core.types

import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.RepresentationId
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class IdVersionedSpec extends WordSpecLike with Matchers with EitherValues {

  private implicit val config = AdminConfig("http://localhost:8080/admin", "projects")

  "An IdVersionedSpec" should {
    val model = IdVersioned(RepresentationId("project", "some/id", "some/schema"), 1L)
    val json = Json.obj(
      "project" -> Json.fromString("http://localhost:8080/admin/projects/project"),
      "@id"     -> Json.fromString("some/id"),
      "schema"  -> Json.fromString("some/schema"),
      "rev"     -> Json.fromLong(1L)
    )

    "encode known events " in {
      model.asJson shouldEqual json
    }

    "decode known events" in {
      json.as[IdVersioned].right.value shouldEqual model
    }
  }
}
