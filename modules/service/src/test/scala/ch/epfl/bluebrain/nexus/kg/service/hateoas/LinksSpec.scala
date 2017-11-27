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
      Links().asJson shouldEqual Json.obj()
    }

    "be added properly" in {
      Links("self" -> Uri(s"$base/link/self")) ++ Links("other" -> Uri(s"$base/link/other")) shouldEqual Links(
        "self"     -> Uri(s"$base/link/self"),
        "other"    -> Uri(s"$base/link/other"))
      Links("self" -> Uri(s"$base/link/self")) + ("other", Uri(s"$base/link/other")) shouldEqual Links(
        "self"     -> Uri(s"$base/link/self"),
        "other"    -> Uri(s"$base/link/other"))
    }

    "be fetched properly" in {
      val links = Links("self" -> Uri(s"$base/link/self"), "other" -> Uri(s"$base/link/other"))
      links.get("self") shouldEqual Some(Uri(s"$base/link/self"))
      links.get("onexisting") shouldEqual None
    }
  }
}
