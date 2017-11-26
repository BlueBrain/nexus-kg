package ch.epfl.bluebrain.nexus.kg.service.io

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder._
import io.circe.{Encoder, Json}
import org.scalatest.{Matchers, WordSpecLike}

class RoutesEncoderOpsSpec extends WordSpecLike with Matchers {

  private val contextUri    = Uri("http://localhost/v0/contexts/nexus/core/resource/v1.0.0")
  private val contextString = Json.fromString("http://localhost/v0/contexts/nexus/core/resource/v1.0.0")

  "RoutesEncoderOps" should {

    "add context when missing" in {
      val input = Json.obj("@id" -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1))
      val output =
        Json.obj("@context" -> contextString, "@id" -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1))
      val dummyEncoder: Encoder[Unit] = Encoder.instance(_ => input)
      dummyEncoder.withContext(contextUri).apply(()) shouldEqual output
    }

    "append context to an existing context value" in {
      val input = Json.obj("@context" -> Json.fromString("http://foo.domain/some/context"),
                           "@id"     -> Json.fromString("foo-id"),
                           "nxv:rev" -> Json.fromLong(1))
      val output = Json.obj(
        "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"), contextString),
        "@id"      -> Json.fromString("foo-id"),
        "nxv:rev"  -> Json.fromLong(1)
      )
      val dummyEncoder: Encoder[Unit] = Encoder.instance(_ => input)
      dummyEncoder.withContext(contextUri).apply(()) shouldEqual output
    }

    "append context to the end of an existing context array " in {
      val input = Json.obj(
        "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                               Json.fromString("http://bar.domain/another/context")),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      )
      val output = Json.obj(
        "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                               Json.fromString("http://bar.domain/another/context"),
                               contextString),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      )
      val dummyEncoder: Encoder[Unit] = Encoder.instance(_ => input)
      dummyEncoder.withContext(contextUri).apply(()) shouldEqual output
    }

    "merge context to an existing context object" in {
      val input = Json.obj(
        "@context" -> Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                               "bar" -> Json.fromString("http://bar.domain/another/context")),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      )
      val output = Json.obj(
        "@context" -> Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                               "bar" -> Json.fromString("http://bar.domain/another/context"),
                               "nxv" -> contextString),
        "@id"     -> Json.fromString("foo-id"),
        "nxv:rev" -> Json.fromLong(1)
      )
      val dummyEncoder: Encoder[Unit] = Encoder.instance(_ => input)
      dummyEncoder.withContext(contextUri).apply(()) shouldEqual output
    }
  }
}
