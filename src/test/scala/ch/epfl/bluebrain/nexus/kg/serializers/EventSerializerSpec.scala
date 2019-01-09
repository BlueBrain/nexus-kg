package ch.epfl.bluebrain.nexus.kg.serializers

import java.nio.charset.Charset
import java.nio.file.Paths
import java.time.Clock
import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes}
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
      Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
         url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)

    val types            = Set(url"https://bbp.epfl.ch/nexus/types/type1".value, url"https://bbp.epfl.ch/nexus/types/type2".value)
    val instant          = Clock.systemUTC.instant()
    val subject: Subject = User("sub:1234", "realm")

    val rep = Map(quote("{timestamp}") -> instant.toString)

    "using EventSerializer" should {
      val value    = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))
      val digest   = Digest("md5", "1234")
      val fileAttr = FileAttributes("uuid", Paths.get("/test/path"), "test-file.json", "application/json", 128L, digest)
      val results = List(
        Created(key, schema, types, value, instant, Anonymous) -> jsonContentOf("/serialization/created-resp.json", rep).noSpaces,
        Deprecated(key, 1L, types, instant, subject)           -> jsonContentOf("/serialization/deprecated-resp.json", rep).noSpaces,
        TagAdded(key, 1L, 2L, "tagName", instant, subject)     -> jsonContentOf("/serialization/tagged-resp.json", rep).noSpaces,
        CreatedFile(key, fileAttr, instant, subject)           -> jsonContentOf("/serialization/created-file-resp.json", rep).noSpaces,
        UpdatedFile(key, 2L, fileAttr, instant, subject)       -> jsonContentOf("/serialization/updated-file-resp.json", rep).noSpaces
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
            serializer.fromBinary(json.getBytes(UTF8), "Event") shouldEqual event
        }
      }
    }
  }
}
