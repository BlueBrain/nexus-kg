package ch.epfl.bluebrain.nexus.kg.core.resources

import java.util.regex.Pattern.quote

import ch.epfl.bluebrain.nexus.commons.test.{Resources => Res}
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.{JsonPayload, TurtlePayload}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{BinaryAttributes, Digest, Size}
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class ResourceSpec extends WordSpecLike with Matchers with EitherValues with Res with Inspectors {

  private implicit val config = AdminConfig("http://localhost/admin", "projects")

  "A Resource" should {
    val id = RepresentationId("project", "http://localhost/some/id", "http://localhost/some/schema")
    val attachment =
      BinaryAttributes("uri", "filename.txt", "text/plain", Size("MB", 123L), Digest("SHA256", "ABCDEF"))
    val jsonPayload   = Json.obj("key" -> Json.obj("nested" -> Json.fromString("value")))
    val turtlePayload = contentOf("/persistence/update.ttl")
    val list = List(
      Resource(id, 1L, JsonPayload(jsonPayload), Set.empty, deprecated = false) -> jsonContentOf("/json/resource.json"),
      Resource(id, 1L, JsonPayload(jsonPayload), Set(attachment), deprecated = false) -> jsonContentOf(
        "/json/resource-at.json",
        Map(quote("{uuid}") -> attachment.uuid)),
      Resource(id, 1L, TurtlePayload(turtlePayload), Set.empty, deprecated = false) -> jsonContentOf(
        "/json/resource-ttl.json")
    )

    "encode known events " in {
      forAll(list) {
        case (model, json) => model.asJson shouldEqual json
      }
    }
  }
}
