package ch.epfl.bluebrain.nexus.kg.serializers

import java.nio.charset.Charset
import java.nio.file.Paths
import java.time.Clock
import java.util.regex.Pattern.quote

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.UserRef
import ch.epfl.bluebrain.nexus.kg.resources.Event.{Created, CreatedBinary, Deprecated, TagAdded}
import ch.epfl.bluebrain.nexus.kg.resources.binary.Binary.{BinaryAttributes, Digest}
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef, Ref, ResId}
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer.EventSerializer
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}
import shapeless.Typeable

class EventSerializerSpec
    extends WordSpecLike
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with OptionValues
    with Randomness
    with Resources {

  private final val UTF8: Charset = Charset.forName("UTF-8")
  private final val serialization = SerializationExtension(system)

  private case class Other(str: String)

  private def findConcreteSerializer[A <: SerializerWithStringManifest](o: AnyRef)(implicit t: Typeable[A]): A =
    t.cast(serialization.findSerializerFor(o)).getOrElse(fail("Expected a SerializerWithManifest"))

  "A Serializer" when {

    val key: ResId =
      Id(ProjectRef("org/projectName"), url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)

    val types              = Set(url"https://bbp.epfl.ch/nexus/types/type1".value, url"https://bbp.epfl.ch/nexus/types/type2".value)
    val instant            = Clock.systemUTC.instant()
    val identity: Identity = UserRef("realm", "sub:1234")

    val rep = Map(quote("{timestamp}") -> instant.toString)

    "using EventSerializer" should {
      val value = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))
      val results = List(
        Created(key, 1L, schema, types, value, instant, identity) -> jsonContentOf("/serialization/created-resp.json",
                                                                                   rep).noSpaces,
        Deprecated(key, 1L, types, instant, identity)       -> jsonContentOf("/serialization/deprecated-resp.json", rep).noSpaces,
        TagAdded(key, 1L, 2L, "tagName", instant, identity) -> jsonContentOf("/serialization/tagged-resp.json", rep).noSpaces,
        CreatedBinary(
          key,
          1L,
          BinaryAttributes(
            "uuid",
            Paths.get("/test/path"),
            "test-file.json",
            "application/json",
            128L,
            Digest("md5", "1234")
          ),
          instant,
          identity
        ) -> jsonContentOf("/serialization/attached-resp.json", rep).noSpaces
      )

      "encode known events to UTF-8" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            new String(serializer.toBinary(event), UTF8) shouldEqual json
            serializer.manifest(event)
        }
      }

      "decode known events" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            serializer.fromBinary(json.getBytes(UTF8), "event") shouldEqual event
        }
      }

    }
  }
}
