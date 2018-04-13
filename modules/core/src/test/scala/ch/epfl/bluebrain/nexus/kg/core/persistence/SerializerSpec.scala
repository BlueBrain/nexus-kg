package ch.epfl.bluebrain.nexus.kg.core.persistence

import java.io.NotSerializableException
import java.nio.charset.Charset
import java.time.Clock

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.core.persistence.Serializer.EventSerializer
import ch.epfl.bluebrain.nexus.kg.core.resources.Event._
import ch.epfl.bluebrain.nexus.kg.core.resources.{Event, RepresentationId}
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.{JsonPayload, _}
import io.circe.{Error, Json}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import shapeless.Typeable
import java.util.regex.Pattern.quote

class SerializerSpec
    extends WordSpecLike
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with Randomness
    with Resources {

  private final val UTF8: Charset = Charset.forName("UTF-8")
  private final val serialization = SerializationExtension(system)

  private case class Other(str: String)

  private def findConcreteSerializer[A <: SerializerWithStringManifest](o: AnyRef)(implicit t: Typeable[A]): A =
    t.cast(serialization.findSerializerFor(o)).getOrElse(fail("Expected a SerializerWithManifest"))

  "A Serializer" when {

    val key: RepresentationId =
      RepresentationId("projectName",
                       "https://bbp.epfl.ch/nexus/data/resourceName",
                       "https://bbp.epfl.ch/nexus/schemas/schemaName")
    val meta = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val tags = Set("project")

    val rep = Map(quote("{timestamp}") -> meta.instant.toString)

    "using EventSerializer" should {
      val value  = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))
      val value2 = contentOf("/persistence/update.ttl")
      val results = List(
        Created(key, 1L, meta, JsonPayload(value), tags)     -> jsonContentOf("/persistence/created-resp.json", rep).noSpaces,
        Replaced(key, 1L, meta, TurtlePayload(value2), tags) -> jsonContentOf("/persistence/replaced-resp.json", rep).noSpaces,
        Deprecated(key, 1L, meta, tags)                      -> jsonContentOf("/persistence/deprecated-resp.json", rep).noSpaces,
        Undeprecated(key, 1L, meta, tags)                    -> jsonContentOf("/persistence/undeprecated-resp.json", rep).noSpaces,
        Tagged(key, 1L, meta, "tagName", tags)               -> jsonContentOf("/persistence/tagged-resp.json", rep).noSpaces
      )

      "encode known events to UTF-8" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            new String(serializer.toBinary(event), UTF8) shouldEqual json
            serializer.manifest(event)
        }
      }

      "failed to encode unexpected event" in {
        val event: Event = Created(key, 1L, meta, JsonPayload(value), tags)
        val serializer   = findConcreteSerializer[EventSerializer](event)
        intercept[IllegalArgumentException](serializer.toBinary(Other("o")))
      }

      "decode known events" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            serializer.fromBinary(json.getBytes(UTF8), "event") shouldEqual event
        }
      }

      "failed to decode event with unexpected manifest" in {
        val event: Event = Created(key, 1L, meta, JsonPayload(value), tags)
        val str          = jsonContentOf("/persistence/created-resp.json", rep).noSpaces
        val serializer   = findConcreteSerializer[EventSerializer](event)
        intercept[NotSerializableException](serializer.fromBinary(str.getBytes(UTF8), "other"))
      }

      "failed to decode wrong event json" in {
        val event: Event = Created(key, 1L, meta, JsonPayload(value), tags)
        val str          = Json.obj("key" -> Json.fromString("value")).noSpaces
        val serializer   = findConcreteSerializer[EventSerializer](event)
        intercept[Error](serializer.fromBinary(str.getBytes(UTF8), "event"))
      }

      "get the manifest of events" in {
        forAll(results) {
          case (event, _) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            serializer.manifest(event) shouldEqual "event"
        }
      }

      "failed when getting manifest of unexpected events" in {
        val event: Event = Created(key, 1L, meta, JsonPayload(value), tags)
        val serializer   = findConcreteSerializer[EventSerializer](event)
        intercept[IllegalArgumentException](serializer.manifest(Other("o")))
      }
    }
  }
}
