package ch.epfl.bluebrain.nexus.kg.service.hateoas

import org.scalatest.{Matchers, WordSpecLike}
import akka.http.scaladsl.model.Uri
import io.circe.Json
import io.circe.syntax._
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links._

class LinksSpec extends WordSpecLike with Matchers {

  "Links" should {

    val base = "http://localhost"
    "be encoded properly into json" in {
      Links("self" -> Uri(s"$base/link/self"), "other" -> Uri(s"$base/link/other")).asJson shouldEqual Json.obj(
        "self"  -> Json.fromString(s"$base/link/self"),
        "other" -> Json.fromString(s"$base/link/other"))

      Links("self" -> Uri(s"$base/link/self"), "some" -> Uri(s"$base/link/one"), "some" -> Uri(s"$base/link/two")).asJson shouldEqual Json
        .obj("self" -> Json.fromString(s"$base/link/self"),
             "some" -> Json.arr(Json.fromString(s"$base/link/one"), Json.fromString(s"$base/link/two")))
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
