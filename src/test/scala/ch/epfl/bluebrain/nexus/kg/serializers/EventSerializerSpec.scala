package ch.epfl.bluebrain.nexus.kg.serializers

import java.nio.charset.Charset
import java.nio.file.Paths
import java.time.Clock
import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.StorageReference.S3StorageReference
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, OrganizationRef, ProjectRef, Ref, ResId}
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer.EventSerializer
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import io.circe.parser._
import org.scalatest._
import shapeless.Typeable

class EventSerializerSpec
    extends WordSpecLike
    with Matchers
    with Inspectors
    with EitherValues
    with ScalatestRouteTest
    with OptionValues
    with Resources
    with TestHelper {

  private final val UTF8: Charset = Charset.forName("UTF-8")
  private final val serialization = SerializationExtension(system)
  private implicit val storageConfig =
    StorageConfig(
      DiskStorageConfig(Paths.get("/tmp/"), "SHA-256", read, write, false),
      RemoteDiskStorageConfig("http://example.com", None, "SHA-256", read, write, true),
      S3StorageConfig("MD5", read, write, true),
      "password",
      "salt"
    )
  private case class Other(str: String)

  private def findConcreteSerializer[A <: SerializerWithStringManifest](o: AnyRef)(implicit t: Typeable[A]): A =
    t.cast(serialization.findSerializerFor(o)).getOrElse(fail("Expected a SerializerWithManifest"))

  "A Serializer" when {

    val key: ResId =
      Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
         url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    val orgRef = OrganizationRef(UUID.fromString("17a62c6a-4dc4-4eaa-b418-42d0634695a1"))

    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)

    val types   = Set(url"https://bbp.epfl.ch/nexus/types/type1".value, url"https://bbp.epfl.ch/nexus/types/type2".value)
    val instant = Clock.systemUTC.instant()
    val subject = User("sub:1234", "realm")

    val rep = Map(quote("{timestamp}") -> instant.toString)

    "using EventSerializer" should {
      val value     = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))
      val storage   = DiskStorage.default(key.parent)
      val digest    = Digest("MD5", "1234")
      val filedUuid = UUID.fromString("b1d7cda2-1ec0-40d2-b12e-3baf4895f7d7")
      val fileAttr =
        FileAttributes(filedUuid,
                       Uri(Paths.get("/test/path").toUri.toString),
                       Uri.Path("path"),
                       "test-file.json",
                       `application/json`,
                       128L,
                       digest)
      val s3fileAttr =
        FileAttributes(filedUuid,
                       Uri("s3://test/path"),
                       Uri.Path("path"),
                       "test-file.json",
                       `application/json`,
                       128L,
                       digest)
      val results = List(
        Created(key, orgRef, schema, types, value, instant, Anonymous) -> jsonContentOf(
          "/serialization/created-resp.json",
          rep),
        Deprecated(key, orgRef, 1L, types, instant, subject) -> jsonContentOf("/serialization/deprecated-resp.json",
                                                                              rep),
        TagAdded(key, orgRef, 1L, 2L, "tagName", instant, subject) -> jsonContentOf("/serialization/tagged-resp.json",
                                                                                    rep),
        FileCreated(key, orgRef, storage.reference, fileAttr, instant, subject) -> jsonContentOf(
          "/serialization/created-file-resp.json",
          rep),
        FileUpdated(key,
                    orgRef,
                    S3StorageReference(url"https://bbp.epfl.ch/nexus/storages/org/proj/s3".value, 2L),
                    2L,
                    s3fileAttr,
                    instant,
                    subject) -> jsonContentOf("/serialization/updated-file-resp.json", rep)
      )

      "encode known events to UTF-8" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            parse(new String(serializer.toBinary(event), UTF8)).right.value shouldEqual json
            serializer.manifest(event)
        }
      }

      "decode known events" in {
        forAll(results) {
          case (event, json) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            serializer.fromBinary(json.noSpaces.getBytes(UTF8), "Event") shouldEqual event
        }
      }
    }
  }
}
