package ch.epfl.bluebrain.nexus.kg.service.hateoas

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.service.io.BaseEncoder
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.linksEncoder
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{Matchers, WordSpecLike}

class LinksSpec extends WordSpecLike with Matchers {

  private val baseEncoder = new BaseEncoder(prefixes)
  import baseEncoder.JsonOps

  "Links" should {

    val base = "http://localhost"
    "be encoded properly into json" in {
      Links("self" -> Uri(s"$base/link/self"), "other" -> Uri(s"$base/link/other")).asJson.addLinksContext shouldEqual Json
        .obj(
          "self"     -> Json.fromString(s"$base/link/self"),
          "other"    -> Json.fromString(s"$base/link/other"),
          "@context" -> Json.fromString("http://localhost/v0/contexts/nexus/core/links/v0.1.0")
        )

      Links("self" -> Uri(s"$base/link/self"), "some" -> Uri(s"$base/link/one"), "some" -> Uri(s"$base/link/two")).asJson.addLinksContext shouldEqual Json
        .obj(
          "self"     -> Json.fromString(s"$base/link/self"),
          "some"     -> Json.arr(Json.fromString(s"$base/link/one"), Json.fromString(s"$base/link/two")),
          "@context" -> Json.fromString("http://localhost/v0/contexts/nexus/core/links/v0.1.0")
        )
    }

    "be added properly" in {
      Links("self" -> Uri(s"$base/link/self")) ++ Links("other" -> Uri(s"$base/link/other")) shouldEqual Links(
        "self"     -> Uri(s"$base/link/self"),
        "other"    -> Uri(s"$base/link/other"))

      Links("self" -> Uri(s"$base/link/self")) + ("other", Uri(s"$base/link/other")) shouldEqual Links(
        "self"     -> Uri(s"$base/link/self"),
        "other"    -> Uri(s"$base/link/other"))

      Links("self" -> Uri(s"$base/link/self")) + ("other", Uri(s"$base/link/other"), Uri(s"$base/link/other2")) shouldEqual Links(
        "self"     -> Uri(s"$base/link/self"),
        "other"    -> Uri(s"$base/link/other"),
        "other"    -> Uri(s"$base/link/other2"))
    }

    "be fetched properly" in {
      val links = Links("self" -> Uri(s"$base/link/self"), "other" -> Uri(s"$base/link/other"))
      links.get("self") shouldEqual Some(List(Uri(s"$base/link/self")))
      links.get("nonexisting") shouldEqual None
    }
  }
}
