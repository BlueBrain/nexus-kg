package ch.epfl.bluebrain.nexus.kg.service.serialization

import java.time.Clock

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.commons.service.io.UTF8
import ch.epfl.bluebrain.nexus.kg.service.contexts.ContextEvent.{ContextCreated, ContextUpdated}
import ch.epfl.bluebrain.nexus.kg.service.contexts.{ContextEvent, ContextId}
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectEvent.{ProjectCreated, ProjectDeprecated, ProjectUpdated}
import ch.epfl.bluebrain.nexus.kg.service.projects.{Project, ProjectEvent, ProjectId}
import ch.epfl.bluebrain.nexus.kg.service.schemas.SchemaEvent.SchemaCreated
import ch.epfl.bluebrain.nexus.kg.service.schemas.{SchemaEvent, SchemaId}
import ch.epfl.bluebrain.nexus.kg.service.serialization.Serializer.EventSerializer
import ch.epfl.bluebrain.nexus.kg.service.serialization.SerializerSpec.DataAndJson
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import shapeless.Typeable

class SerializerSpec extends WordSpecLike with Matchers with Inspectors with ScalatestRouteTest {

  val serialization = SerializationExtension(system)

  def findConcreteSerializer[A <: SerializerWithStringManifest](o: AnyRef)(implicit t: Typeable[A]): A =
    t.cast(serialization.findSerializerFor(o)).getOrElse(fail("Expected a SerializerWithManifest"))

  "A Serializer" when {

    val projectId = ProjectId("projectid").get
    val schemaId  = SchemaId(projectId, "schemaId").get
    val contextId = ContextId(projectId, "contextId").get
    val meta      = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val metaAnon  = Meta(Anonymous(), Clock.systemUTC.instant())

    "using EventSerializer" should {
      val results = List(
        DataAndJson[ProjectEvent](
          ProjectCreated(projectId,
                         1,
                         meta,
                         Json.obj("key" -> Json.fromString("http://localhost.com/path/")),
                         Project.Config(10L)),
          s"""{"id":"projectid","rev":1,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"@context":{"key":"http://localhost.com/path/"},"config":{"maxAttachmentSize":10},"type":"ProjectCreated"}"""
        ),
        DataAndJson[ProjectEvent](
          ProjectUpdated(projectId,
                         2,
                         meta,
                         Json.obj("key2" -> Json.fromString("http://localhost.com/path2/")),
                         Project.Config(20L)),
          s"""{"id":"projectid","rev":2,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"@context":{"key2":"http://localhost.com/path2/"},"config":{"maxAttachmentSize":20},"type":"ProjectUpdated"}"""
        ),
        DataAndJson[ProjectEvent](
          ProjectDeprecated(projectId, 3, meta),
          s"""{"id":"projectid","rev":3,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"type":"ProjectDeprecated"}"""
        ),
        DataAndJson[SchemaEvent](
          SchemaCreated(schemaId, 1L, metaAnon, Json.obj("key" -> Json.fromString("value"))),
          s"""{"id":"projectid/schemaId","rev":1,"meta":{"author":{"id":"anonymous","type":"Anonymous"},"instant":"${metaAnon.instant}"},"value":{"key":"value"},"type":"SchemaCreated"}"""
        ),
        DataAndJson[SchemaEvent](
          SchemaCreated(schemaId, 1, meta, Json.obj()),
          s"""{"id":"projectid/schemaId","rev":1,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{},"type":"SchemaCreated"}"""
        ),
        DataAndJson[ContextEvent](
          ContextCreated(contextId, 1, meta, Json.obj()),
          s"""{"id":"projectid/contextId","rev":1,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{},"type":"ContextCreated"}"""
        ),
        DataAndJson[ContextEvent](
          ContextUpdated(contextId, 1, meta, Json.obj("key" -> Json.fromString("value"))),
          s"""{"id":"projectid/contextId","rev":1,"meta":{"author":{"id":"realms/realm/users/sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{"key":"value"},"type":"ContextUpdated"}"""
        )
      )

      "encode known events to UTF-8" in {
        forAll(results) {
          case DataAndJson(event, json, _) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            new String(serializer.toBinary(event), UTF8) shouldEqual json
        }
      }

      "decode known events" in {
        forAll(results) {
          case DataAndJson(event, json, manifest) =>
            val serializer = findConcreteSerializer[EventSerializer](event)
            serializer.fromBinary(json.getBytes(UTF8), manifest) shouldEqual event
        }
      }
    }
  }
}

object SerializerSpec {

  /**
    * Holds both the JSON representation and the data structure
    *
    * @param data     instance of the data as a data structure
    * @param json     the JSON representation of the data
    * @param manifest the manifest to be used for selecting the appropriate resulting type
    */
  case class DataAndJson[A](data: A, json: String, manifest: String)

  object DataAndJson {
    def apply[A](data: A, json: String)(implicit tb: Typeable[A]): DataAndJson[A] =
      DataAndJson[A](data, json, tb.describe)
  }

}
