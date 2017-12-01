package ch.epfl.bluebrain.nexus.kg.service.io

import io.circe.Json
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class JsonOpsSpec extends WordSpecLike with Matchers with Inspectors {

  private val contextUri    = prefixes.CoreContext
  private val contextString = Json.fromString(contextUri.toString)
  private val baseEncoder   = new BaseEncoder(prefixes)
  import baseEncoder._

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
          in.addCoreContext shouldEqual out
      }
    }
  }
}
