package ch.epfl.bluebrain.nexus.kg.service.io

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonOps
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class JsonOpsSpec extends WordSpecLike with Matchers with Inspectors {

  private val contextUri    = Uri("http://localhost/v0/contexts/nexus/core/standards/v0.1.0")
  private val contextString = Json.fromString("http://localhost/v0/contexts/nexus/core/standards/v0.1.0")

  private val mapping = List(
    Json.obj("@id"        -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1)) ->
      Json.obj("@context" -> contextString, "@id" -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1)),
    Json.obj("@context"   -> Json.fromString("http://foo.domain/some/context"),
             "@id"        -> Json.fromString("foo-id"),
             "nxv:rev"    -> Json.fromLong(1)) ->
      Json.obj(
        "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"), contextString),
        "@id"      -> Json.fromString("foo-id"),
        "nxv:rev"  -> Json.fromLong(1)
      ),
    Json.obj(
      "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                             Json.fromString("http://bar.domain/another/context")),
      "@id"     -> Json.fromString("foo-id"),
      "nxv:rev" -> Json.fromLong(1)
    ) ->
      Json.obj(
        "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                               Json.fromString("http://bar.domain/another/context"),
                               contextString),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      ),
    Json.obj(
      "@context" -> Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                             "bar" -> Json.fromString("http://bar.domain/another/context")),
      "@id"     -> Json.fromString("foo-id"),
      "nxv:rev" -> Json.fromLong(1)
    ) ->
      Json.obj(
        "@context" -> Json.arr(Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                                        "bar" -> Json.fromString("http://bar.domain/another/context")),
                               contextString),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      )
  )

  "JsonOps" should {
    "properly add or merge context into JSON payload" in {
      forAll(mapping) {
        case (in, out) =>
          in.addContext(contextUri) shouldEqual out
      }
    }
  }
}
