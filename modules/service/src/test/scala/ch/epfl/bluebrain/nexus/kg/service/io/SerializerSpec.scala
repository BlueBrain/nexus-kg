package ch.epfl.bluebrain.nexus.kg.service.io

import java.time.Clock
import java.util.UUID

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.DomainCreated
import ch.epfl.bluebrain.nexus.kg.core.domains._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent.InstanceCreated
import ch.epfl.bluebrain.nexus.kg.core.instances._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated, OrgUpdated}
import ch.epfl.bluebrain.nexus.kg.core.organizations._
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent.SchemaCreated
import ch.epfl.bluebrain.nexus.kg.core.schemas._
import ch.epfl.bluebrain.nexus.kg.service.io.Serializer.EventSerializer
import ch.epfl.bluebrain.nexus.kg.service.io.SerializerSpec.DataAndJson
import ch.epfl.bluebrain.nexus.commons.service.io.UTF8
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, ContextId}
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent.ContextCreated
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import shapeless.Typeable

class SerializerSpec extends WordSpecLike with Matchers with Inspectors with ScalatestRouteTest {

  val serialization = SerializationExtension(system)

  def findConcreteSerializer[A <: SerializerWithStringManifest](o: AnyRef)(implicit t: Typeable[A]): A = {
    t.cast(serialization.findSerializerFor(o))
      .getOrElse(fail("Expected a SerializerWithManifest"))
  }

  "A Serializer" when {

    val uuid     = UUID.randomUUID().toString
    val domainId = DomainId(OrgId("orgid"), "domainid")
    val meta     = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val metaAnon = Meta(Anonymous, Clock.systemUTC.instant())

    "using EventSerializer" should {
      val results = List(
        DataAndJson[OrgEvent](
          OrgCreated(OrgId("orgid"), 1, meta, Json.obj()),
          s"""{"id":"orgid","rev":1,"meta":{"author":{"realm":"realm","sub":"sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{},"type":"OrgCreated"}"""
        ),
        DataAndJson[OrgEvent](
          OrgUpdated(OrgId("orgid"), 2, meta, Json.obj("one" -> Json.fromString("two"))),
          s"""{"id":"orgid","rev":2,"meta":{"author":{"realm":"realm","sub":"sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{"one":"two"},"type":"OrgUpdated"}"""
        ),
        DataAndJson[OrgEvent](
          OrgDeprecated(OrgId("orgid"), 3, metaAnon),
          s"""{"id":"orgid","rev":3,"meta":{"author":{"type":"Anonymous"},"instant":"${metaAnon.instant}"},"type":"OrgDeprecated"}"""
        ),
        DataAndJson[DomainEvent](
          DomainCreated(domainId, 1L, metaAnon, "desc"),
          s"""{"id":"orgid/domainid","rev":1,"meta":{"author":{"type":"Anonymous"},"instant":"${metaAnon.instant}"},"description":"desc","type":"DomainCreated"}"""
        ),
        DataAndJson[SchemaEvent](
          SchemaCreated(SchemaId(domainId, "schemaname", Version(1, 1, 1)), 1, meta, Json.obj()),
          s"""{"id":"orgid/domainid/schemaname/v1.1.1","rev":1,"meta":{"author":{"realm":"realm","sub":"sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{},"type":"SchemaCreated"}"""
        ),
        DataAndJson[ContextEvent](
          ContextCreated(ContextId(domainId, "contextname", Version(1, 1, 1)), 1, meta, Json.obj()),
          s"""{"id":"orgid/domainid/contextname/v1.1.1","rev":1,"meta":{"author":{"realm":"realm","sub":"sub:1234","type":"UserRef"},"instant":"${meta.instant}"},"value":{},"type":"ContextCreated"}"""
        ),
        DataAndJson[InstanceEvent](
          InstanceCreated(InstanceId(SchemaId(domainId, "schemaname", Version(1, 1, 1)), uuid),
                          1,
                          metaAnon,
                          Json.obj()),
          s"""{"id":"orgid/domainid/schemaname/v1.1.1/$uuid","rev":1,"meta":{"author":{"type":"Anonymous"},"instant":"${metaAnon.instant}"},"value":{},"type":"InstanceCreated"}"""
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
          case data @ DataAndJson(event, json, manifest) =>
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
